package learn.query.catalog;

import learn.query.Fixtures;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableLoaderTest {

    @Test
    void loadsCsvWithInferredTypes() throws Exception {
        Table table = TableLoader.load(Fixtures.path("sales.csv"));

        assertEquals("SALES", table.name());
        assertEquals(List.of(
            new Column("REGION", SqlTypeName.VARCHAR),
            new Column("SALES", SqlTypeName.BIGINT)), table.columns());
        assertEquals(8, table.rows().size());
        assertArrayEquals(new Object[] {"east", 10L}, table.rows().get(0));
    }

    @Test
    void parquetHasTheSameColumnsAndRowsAsCsv() throws Exception {
        Table csv = TableLoader.load(Fixtures.path("sales.csv"));
        Table parquet = TableLoader.load(Fixtures.path("sales.parquet"));

        assertEquals(csv.columns(), parquet.columns());
        assertEquals(asLists(csv), asLists(parquet));
    }

    @Test
    void tableNameComesFromTheFileName(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("orders.csv");
        Files.writeString(file, "customer,amount\nann,5\nbob,7\n");

        Catalog catalog = Catalog.load(List.of(file));
        Table orders = catalog.get("ORDERS");

        assertNotNull(orders);
        assertEquals(List.of(
            new Column("CUSTOMER", SqlTypeName.VARCHAR),
            new Column("AMOUNT", SqlTypeName.BIGINT)), orders.columns());
    }

    @Test
    void rejectsUnknownFileType() {
        assertThrows(IllegalArgumentException.class, () -> TableLoader.load(Path.of("sales.json")));
    }

    @Test
    void rejectsRaggedCsv(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("bad.csv");
        Files.writeString(file, "region,sales\neast,10,extra\n");

        assertThrows(IllegalArgumentException.class, () -> TableLoader.load(file));
    }

    private static List<List<Object>> asLists(Table table) {
        List<List<Object>> out = new ArrayList<>();
        for (Object[] row : table.rows()) {
            out.add(List.of(row));
        }
        return out;
    }
}
