package learn.query.sql;

import learn.query.Fixtures;
import learn.query.catalog.Catalog;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlEngineTest {

    @Test
    void groupedSumOverSampleDataMatchesHandComputedTotals() throws Exception {
        Catalog catalog = Catalog.load(List.of(Fixtures.path("sales.csv")));

        try (SqlEngine engine = new SqlEngine(catalog)) {
            List<List<Object>> result = engine.query(
                "SELECT REGION, SUM(SALES) AS T FROM SALES GROUP BY REGION ORDER BY REGION");

            assertEquals(List.of(
                List.of("east", 19L),
                List.of("north", 11L),
                List.of("west", 28L)), result);
        }
    }

    @Test
    void ungroupedAggregateOverEmptyTableReturnsOneRowWithNulls() throws Exception {
        try (SqlEngine engine = new SqlEngine(Fixtures.salesCatalog(List.of()))) {
            List<List<Object>> result = engine.query("SELECT SUM(SALES), COUNT(*) FROM SALES");

            assertEquals(1, result.size());
            assertNull(result.get(0).get(0));
            assertEquals(0L, result.get(0).get(1));
        }
    }
}
