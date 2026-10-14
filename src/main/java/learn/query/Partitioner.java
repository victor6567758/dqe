package learn.query;

import learn.query.catalog.Catalog;
import learn.query.catalog.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Splits data into contiguous chunks, one per worker. */
public final class Partitioner {

    private Partitioner() {}

    public static <T> List<List<T>> split(List<T> rows, int partitions) {
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be at least 1");
        }
        int size = (rows.size() + partitions - 1) / partitions; // ceiling division
        List<List<T>> out = new ArrayList<>();
        for (int start = 0; start < rows.size(); start += size) {
            int end = Math.min(start + size, rows.size());
            out.add(List.copyOf(rows.subList(start, end)));
        }
        return out;
    }

    /** Splits every table in the catalog, and gives each worker its chunk of every table. */
    public static List<Partition> partition(Catalog catalog, int partitions) {
        Map<String, List<List<Object[]>>> chunks = new LinkedHashMap<>();
        for (Table table : catalog.tables()) {
            chunks.put(table.name(), split(table.rows(), partitions));
        }
        List<Partition> out = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            Map<String, List<Object[]>> tables = new LinkedHashMap<>();
            for (Map.Entry<String, List<List<Object[]>>> entry : chunks.entrySet()) {
                List<List<Object[]>> tableChunks = entry.getValue();
                tables.put(entry.getKey(), i < tableChunks.size() ? tableChunks.get(i) : List.of());
            }
            out.add(new Partition(i, tables));
        }
        return out;
    }
}
