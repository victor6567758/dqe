package learn.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts partial aggregate states to and from Arrow record batches, the wire format between fragments.
 * Layout: one key column per grouping column (VARCHAR or BIGINT), then one BIGINT state column per aggregate.
 */
public final class ArrowBatches {

    private ArrowBatches() {}

    public static VectorSchemaRoot fromPartials(BufferAllocator allocator, List<SqlTypeName> keyTypes,
                                                Map<List<Object>, long[]> partials, int stateCount) {
        List<FieldVector> vectors = new ArrayList<>();
        try {
            for (int i = 0; i < keyTypes.size(); i++) {
                vectors.add(newKeyVector("key" + i, keyTypes.get(i), allocator));
            }
            for (int i = 0; i < stateCount; i++) {
                vectors.add(new BigIntVector("state" + i, allocator));
            }
            for (FieldVector vector : vectors) {
                vector.allocateNew();
            }

            int row = 0;
            for (Map.Entry<List<Object>, long[]> entry : partials.entrySet()) {
                for (int i = 0; i < keyTypes.size(); i++) {
                    writeKey(vectors.get(i), row, entry.getKey().get(i));
                }
                for (int i = 0; i < stateCount; i++) {
                    ((BigIntVector) vectors.get(keyTypes.size() + i)).setSafe(row, entry.getValue()[i]);
                }
                row++;
            }
            for (FieldVector vector : vectors) {
                vector.setValueCount(row);
            }
        } catch (RuntimeException e) {
            // Release whatever was already allocated so the allocator can close cleanly.
            vectors.forEach(FieldVector::close);
            throw e;
        }
        return new VectorSchemaRoot(vectors);
    }

    public static Map<List<Object>, long[]> toPartials(VectorSchemaRoot root, int keyCount, int stateCount) {
        Map<List<Object>, long[]> out = new LinkedHashMap<>();
        for (int row = 0; row < root.getRowCount(); row++) {
            List<Object> key = new ArrayList<>(keyCount);
            for (int i = 0; i < keyCount; i++) {
                key.add(readKey(root.getVector("key" + i), row));
            }
            long[] states = new long[stateCount];
            for (int i = 0; i < stateCount; i++) {
                states[i] = ((BigIntVector) root.getVector("state" + i)).get(row);
            }
            out.put(key, states);
        }
        return out;
    }

    private static FieldVector newKeyVector(String name, SqlTypeName type, BufferAllocator allocator) {
        return switch (type) {
            case VARCHAR -> new VarCharVector(name, allocator);
            case BIGINT -> new BigIntVector(name, allocator);
            default -> throw new IllegalArgumentException("Unsupported grouping type: " + type);
        };
    }

    private static void writeKey(FieldVector vector, int row, Object value) {
        if (vector instanceof VarCharVector varchar) {
            if (value == null) {
                varchar.setNull(row);
            } else {
                varchar.setSafe(row, ((String) value).getBytes(StandardCharsets.UTF_8));
            }
        } else {
            BigIntVector bigint = (BigIntVector) vector;
            if (value == null) {
                bigint.setNull(row);
            } else {
                bigint.setSafe(row, ((Long) value).longValue());
            }
        }
    }

    private static Object readKey(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        if (vector instanceof VarCharVector varchar) {
            return new String(varchar.get(row), StandardCharsets.UTF_8);
        }
        return ((BigIntVector) vector).get(row);
    }
}
