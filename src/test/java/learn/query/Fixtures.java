package learn.query;

import learn.query.catalog.Catalog;
import learn.query.catalog.Column;
import learn.query.catalog.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;

/** Test resources, and helpers that build tables for the tests. */
public final class Fixtures {

    private Fixtures() {}

    public static Path path(String name) throws URISyntaxException {
        var url = Fixtures.class.getResource("/" + name);
        if (url == null) {
            throw new IllegalStateException("Missing test resource: " + name);
        }
        return Path.of(url.toURI());
    }

    /** The CSV fixture loaded through the catalog: table SALES, 8 rows. */
    public static Catalog salesFixture() throws Exception {
        return Catalog.load(List.of(path("sales.csv")));
    }

    /** A SALES table with the given rows. Each row is {region, sales}. */
    public static Catalog salesCatalog(List<Object[]> rows) {
        return Catalog.of(new Table("SALES",
            List.of(new Column("REGION", SqlTypeName.VARCHAR), new Column("SALES", SqlTypeName.BIGINT)),
            rows));
    }
}
