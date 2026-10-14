package learn.query.catalog;

import java.util.List;

/** A table definition with its rows. Each row is an Object[] in column order: Long, String, or null. */
public record Table(String name, List<Column> columns, List<Object[]> rows) {

    public Table {
        columns = List.copyOf(columns);
        rows = List.copyOf(rows);
    }
}
