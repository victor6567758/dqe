package learn.query;

import java.util.List;
import java.util.Map;

/**
 * One worker's share of the data: the rows of every table, restricted to this partition.
 * Table names are the keys of the map.
 */
public record Partition(int index, Map<String, List<Object[]>> tables) {

    public List<Object[]> rows(String table) {
        List<Object[]> rows = tables.get(table);
        if (rows == null) {
            throw new IllegalArgumentException("Partition " + index + " has no table " + table);
        }
        return rows;
    }

    public int rowCount() {
        return tables.values().stream().mapToInt(List::size).sum();
    }
}
