package learn.query.catalog;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Reads a CSV or Parquet file into a Table.
 * CSV: the first line is the header; a column is BIGINT when every value parses as an integer,
 * otherwise VARCHAR. Empty values are null. Quoted fields are not supported.
 * Parquet: the schema comes from the file. Only string and 64-bit integer columns are supported.
 */
public final class TableLoader {

    private static final long BATCH_SIZE = 32_768;

    private TableLoader() {}

    public static Table load(Path file) throws IOException {
        String name = tableName(file);
        String lower = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (lower.endsWith(".csv")) {
            return loadCsv(name, file);
        }
        if (lower.endsWith(".parquet")) {
            return loadParquet(name, file);
        }
        throw new IllegalArgumentException("Unsupported file type: " + file);
    }

    static String tableName(Path file) {
        String fileName = file.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String base = dot > 0 ? fileName.substring(0, dot) : fileName;
        return base.toUpperCase(Locale.ROOT);
    }

    private static Table loadCsv(String name, Path file) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Empty CSV file: " + file);
        }
        String[] header = splitLine(lines.get(0));

        List<String[]> cells = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            if (lines.get(i).isBlank()) {
                continue;
            }
            String[] parts = splitLine(lines.get(i));
            if (parts.length != header.length) {
                throw new IllegalArgumentException("Line " + (i + 1) + ": expected " + header.length
                    + " fields, got " + parts.length);
            }
            cells.add(parts);
        }

        List<Column> columns = new ArrayList<>();
        for (int c = 0; c < header.length; c++) {
            columns.add(new Column(header[c].toUpperCase(Locale.ROOT), inferType(cells, c)));
        }

        List<Object[]> rows = new ArrayList<>();
        for (String[] parts : cells) {
            Object[] values = new Object[header.length];
            for (int c = 0; c < header.length; c++) {
                values[c] = parse(parts[c], columns.get(c).type());
            }
            rows.add(values);
        }
        return new Table(name, columns, rows);
    }

    private static String[] splitLine(String line) {
        String[] parts = line.split(",", -1);
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].trim();
        }
        return parts;
    }

    private static SqlTypeName inferType(List<String[]> cells, int column) {
        for (String[] parts : cells) {
            String value = parts[column];
            if (value.isEmpty()) {
                continue;
            }
            try {
                Long.parseLong(value);
            } catch (NumberFormatException e) {
                return SqlTypeName.VARCHAR;
            }
        }
        return SqlTypeName.BIGINT;
    }

    private static Object parse(String value, SqlTypeName type) {
        if (value.isEmpty()) {
            return null;
        }
        if (type == SqlTypeName.BIGINT) {
            return Long.parseLong(value);
        }
        return value;
    }

    private static Table loadParquet(String name, Path file) throws IOException {
        String uri = file.toAbsolutePath().toUri().toString();
        try (BufferAllocator allocator = new RootAllocator();
             DatasetFactory factory = new FileSystemDatasetFactory(
                 allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
             Dataset dataset = factory.finish();
             Scanner scanner = dataset.newScan(new ScanOptions(BATCH_SIZE));
             ArrowReader batches = scanner.scanBatches()) {

            List<Column> columns = null;
            List<String> sourceNames = null;
            List<Object[]> rows = new ArrayList<>();
            while (batches.loadNextBatch()) {
                VectorSchemaRoot root = batches.getVectorSchemaRoot();
                if (columns == null) {
                    // The column definitions come from the first batch's schema.
                    columns = new ArrayList<>();
                    sourceNames = new ArrayList<>();
                    for (Field field : root.getSchema().getFields()) {
                        columns.add(new Column(field.getName().toUpperCase(Locale.ROOT), sqlType(field)));
                        sourceNames.add(field.getName());
                    }
                }
                for (int r = 0; r < root.getRowCount(); r++) {
                    Object[] values = new Object[columns.size()];
                    for (int c = 0; c < columns.size(); c++) {
                        values[c] = readValue(root.getVector(sourceNames.get(c)), r);
                    }
                    rows.add(values);
                }
            }
            if (columns == null) {
                throw new IOException("Parquet file has no record batches: " + file);
            }
            return new Table(name, columns, rows);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            // Arrow's dataset classes declare close() as throws Exception; report it as an IO failure.
            throw new IOException("Failed to read Parquet file " + file, e);
        }
    }

    private static SqlTypeName sqlType(Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Utf8) {
            return SqlTypeName.VARCHAR;
        }
        if (type instanceof ArrowType.Int integer && integer.getBitWidth() == 64) {
            return SqlTypeName.BIGINT;
        }
        throw new IllegalArgumentException("Unsupported Parquet type for column " + field.getName() + ": " + type);
    }

    private static Object readValue(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        if (vector instanceof VarCharVector varchar) {
            return new String(varchar.get(row), StandardCharsets.UTF_8);
        }
        return ((BigIntVector) vector).get(row);
    }
}
