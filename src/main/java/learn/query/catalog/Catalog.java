package learn.query.catalog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** The set of tables a query can reference. Planning and execution both read from it. */
public final class Catalog {

    private final Map<String, Table> tables = new LinkedHashMap<>();

    public static Catalog of(Table... tables) {
        Catalog catalog = new Catalog();
        for (Table table : tables) {
            catalog.add(table);
        }
        return catalog;
    }

    /** Loads one table per file. The table name is the file name without its extension. */
    public static Catalog load(List<Path> files) throws IOException {
        Catalog catalog = new Catalog();
        for (Path file : files) {
            catalog.add(TableLoader.load(file));
        }
        return catalog;
    }

    public void add(Table table) {
        String key = table.name().toUpperCase(Locale.ROOT);
        if (tables.containsKey(key)) {
            throw new IllegalArgumentException("Duplicate table: " + key);
        }
        tables.put(key, table);
    }

    public Table get(String name) {
        return tables.get(name.toUpperCase(Locale.ROOT));
    }

    public Collection<Table> tables() {
        return tables.values();
    }
}
