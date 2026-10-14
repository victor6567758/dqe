package learn.query.sql;

import learn.query.catalog.Catalog;
import learn.query.catalog.Table;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Calcite's own engine over the catalog. The tests use it as a reference for the distributed
 * path. The production path plans and executes queries itself.
 */
public class SqlEngine implements AutoCloseable {

    private final Connection connection;

    public SqlEngine(Catalog catalog) throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
        SchemaPlus root = calcite.getRootSchema();
        for (Table table : catalog.tables()) {
            root.add(table.name(), new CatalogTable(table));
        }
    }

    /** Runs one SQL query and returns its rows. Values are Long, String, or null. */
    public List<List<Object>> query(String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            int columns = rs.getMetaData().getColumnCount();
            List<List<Object>> out = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>(columns);
                for (int c = 1; c <= columns; c++) {
                    row.add(rs.getObject(c));
                }
                out.add(row);
            }
            return out;
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
