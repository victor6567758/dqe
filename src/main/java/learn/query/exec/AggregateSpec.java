package learn.query.exec;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes one Aggregate node in a form the workers and the coordinator can execute:
 * the grouping columns, the key types, and the aggregate calls.
 * Supports SUM, MIN, MAX, and COUNT over BIGINT columns, and grouping by VARCHAR or BIGINT.
 */
public final class AggregateSpec {

    /** One aggregate call. argIndex is the input column, or -1 for COUNT(*). */
    public record AggCall(SqlKind kind, int argIndex) {}

    public static final Comparator<List<Object>> KEY_ORDER = AggregateSpec::compareKeys;

    private final List<Integer> groupIndexes;
    private final List<SqlTypeName> keyTypes;
    private final List<AggCall> calls;

    private AggregateSpec(List<Integer> groupIndexes, List<SqlTypeName> keyTypes, List<AggCall> calls) {
        this.groupIndexes = groupIndexes;
        this.keyTypes = keyTypes;
        this.calls = calls;
    }

    public static AggregateSpec from(Aggregate aggregate) {
        if (aggregate.getGroupSets().size() != 1) {
            throw new UnsupportedOperationException("GROUPING SETS, CUBE, and ROLLUP are not supported");
        }
        List<RelDataTypeField> inputFields = aggregate.getInput().getRowType().getFieldList();

        List<Integer> groupIndexes = new ArrayList<>();
        for (int index : aggregate.getGroupSet()) {
            groupIndexes.add(index);
        }

        List<SqlTypeName> keyTypes = new ArrayList<>();
        for (int index : groupIndexes) {
            SqlTypeName type = inputFields.get(index).getType().getSqlTypeName();
            if (type != SqlTypeName.VARCHAR && type != SqlTypeName.BIGINT) {
                throw new UnsupportedOperationException("Grouping supports VARCHAR and BIGINT columns, not " + type);
            }
            keyTypes.add(type);
        }

        List<AggCall> calls = new ArrayList<>();
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.isDistinct()) {
                throw new UnsupportedOperationException("DISTINCT aggregates are not supported");
            }
            SqlKind kind = call.getAggregation().getKind();
            if (kind != SqlKind.SUM && kind != SqlKind.MIN && kind != SqlKind.MAX && kind != SqlKind.COUNT) {
                throw new UnsupportedOperationException("Unsupported aggregate: " + call.getAggregation().getName());
            }
            int argIndex = call.getArgList().isEmpty() ? -1 : call.getArgList().get(0);
            if (kind != SqlKind.COUNT
                    && (argIndex < 0 || inputFields.get(argIndex).getType().getSqlTypeName() != SqlTypeName.BIGINT)) {
                throw new UnsupportedOperationException(kind + " is supported over BIGINT columns only");
            }
            calls.add(new AggCall(kind, argIndex));
        }
        return new AggregateSpec(groupIndexes, keyTypes, calls);
    }

    public List<SqlTypeName> keyTypes() {
        return keyTypes;
    }

    public List<AggCall> calls() {
        return calls;
    }

    /** Partial aggregation on one worker: one state per group. */
    public Map<List<Object>, long[]> partialStates(List<Object[]> rows) {
        Map<List<Object>, long[]> partial = new HashMap<>();
        for (Object[] row : rows) {
            List<Object> key = new ArrayList<>(groupIndexes.size());
            for (int index : groupIndexes) {
                key.add(row[index]);
            }
            long[] state = partial.computeIfAbsent(key, k -> initialState());
            for (int i = 0; i < calls.size(); i++) {
                state[i] = accumulate(calls.get(i), state[i], row);
            }
        }
        return partial;
    }

    /** Final merge on the coordinator: SUM and COUNT add up, MIN and MAX keep the extreme. */
    public long[] mergeStates(long[] a, long[] b) {
        long[] out = new long[a.length];
        for (int i = 0; i < a.length; i++) {
            out[i] = switch (calls.get(i).kind()) {
                case SUM, COUNT -> a[i] + b[i];
                case MIN -> Math.min(a[i], b[i]);
                case MAX -> Math.max(a[i], b[i]);
                default -> throw new IllegalStateException("Unexpected aggregate");
            };
        }
        return out;
    }

    /** Output rows in Calcite's Aggregate layout: grouping columns first, then aggregate results. */
    public List<Object[]> finalRows(Map<List<Object>, long[]> merged) {
        List<Object[]> rows = new ArrayList<>();
        if (merged.isEmpty() && groupIndexes.isEmpty()) {
            // An ungrouped aggregate returns one row even when no input rows exist.
            Object[] row = new Object[calls.size()];
            for (int i = 0; i < calls.size(); i++) {
                row[i] = calls.get(i).kind() == SqlKind.COUNT ? (Object) 0L : null;
            }
            rows.add(row);
            return rows;
        }
        for (Map.Entry<List<Object>, long[]> entry : merged.entrySet()) {
            Object[] row = new Object[groupIndexes.size() + calls.size()];
            for (int k = 0; k < groupIndexes.size(); k++) {
                row[k] = entry.getKey().get(k);
            }
            for (int i = 0; i < calls.size(); i++) {
                row[groupIndexes.size() + i] = entry.getValue()[i];
            }
            rows.add(row);
        }
        return rows;
    }

    private long[] initialState() {
        long[] state = new long[calls.size()];
        for (int i = 0; i < calls.size(); i++) {
            state[i] = switch (calls.get(i).kind()) {
                case SUM, COUNT -> 0L;
                case MIN -> Long.MAX_VALUE;
                case MAX -> Long.MIN_VALUE;
                default -> throw new IllegalStateException("Unexpected aggregate");
            };
        }
        return state;
    }

    private static long accumulate(AggCall call, long current, Object[] row) {
        Object value = call.argIndex() < 0 ? (Object) 1L : row[call.argIndex()];
        if (value == null) {
            return current;
        }
        return switch (call.kind()) {
            case COUNT -> current + 1;
            case SUM -> current + (Long) value;
            case MIN -> Math.min(current, (Long) value);
            case MAX -> Math.max(current, (Long) value);
            default -> throw new IllegalStateException("Unexpected aggregate");
        };
    }

    @SuppressWarnings("unchecked")
    private static int compareKeys(List<Object> a, List<Object> b) {
        for (int i = 0; i < a.size(); i++) {
            Object x = a.get(i);
            Object y = b.get(i);
            int c = (x == null || y == null) ? (x == null ? (y == null ? 0 : -1) : 1) : ((Comparable<Object>) x).compareTo(y);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }
}
