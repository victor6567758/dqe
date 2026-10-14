package learn.query;

import learn.query.catalog.Catalog;
import learn.query.catalog.Column;
import learn.query.catalog.Table;
import learn.query.sql.QueryPlanner;
import learn.query.sql.SqlEngine;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorTest {

    private static final int WORKERS = 3;

    private static List<List<Object>> run(Catalog catalog, String sql) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
             Coordinator coordinator = new Coordinator(allocator, Partitioner.partition(catalog, WORKERS), WORKERS)) {
            return coordinator.execute(QueryPlanner.plan(sql, catalog));
        }
    }

    @Test
    void sumByRegionOverSampleData() throws Exception {
        assertEquals(List.of(
            List.of("east", 19L),
            List.of("north", 11L),
            List.of("west", 28L)),
            run(Fixtures.salesFixture(),
                "SELECT REGION, SUM(SALES) FROM SALES GROUP BY REGION ORDER BY REGION"));
    }

    @Test
    void mixedAggregatesFollowSelectListOrder() throws Exception {
        assertEquals(List.of(
            List.of("east", 10L, 3L, 4L),
            List.of("north", 8L, 2L, 3L),
            List.of("west", 20L, 3L, 1L)),
            run(Fixtures.salesFixture(),
                "SELECT REGION, MAX(SALES), COUNT(*), MIN(SALES) FROM SALES GROUP BY REGION ORDER BY REGION"));
    }

    @Test
    void whereClauseFiltersBeforeAggregation() throws Exception {
        assertEquals(List.of(
            List.of("east", 2L),
            List.of("north", 1L),
            List.of("west", 2L)),
            run(Fixtures.salesFixture(),
                "SELECT REGION, COUNT(*) FROM SALES WHERE SALES >= 5 GROUP BY REGION ORDER BY REGION"));
    }

    @Test
    void havingFiltersGroupsAboveTheExchange() throws Exception {
        assertEquals(List.of(
            List.of("east", 19L),
            List.of("west", 28L)),
            run(Fixtures.salesFixture(),
                "SELECT REGION, SUM(SALES) FROM SALES GROUP BY REGION HAVING SUM(SALES) > 15 ORDER BY REGION"));
    }

    @Test
    void ungroupedAggregateOverNoRowsReturnsOneRow() throws Exception {
        List<List<Object>> result = run(Fixtures.salesFixture(),
            "SELECT SUM(SALES), COUNT(*) FROM SALES WHERE SALES > 1000");

        assertEquals(1, result.size());
        assertNull(result.get(0).get(0));
        assertEquals(0L, result.get(0).get(1));
    }

    @Test
    void runsAQueryOnATableOutsideTheSalesSchema() throws Exception {
        Catalog orders = Catalog.of(new Table("ORDERS",
            List.of(new Column("CUSTOMER", SqlTypeName.VARCHAR), new Column("AMOUNT", SqlTypeName.BIGINT)),
            List.of(new Object[] {"ann", 5L}, new Object[] {"bob", 7L}, new Object[] {"ann", 3L})));

        assertEquals(List.of(
            List.of("ann", 8L),
            List.of("bob", 7L)),
            run(orders, "SELECT CUSTOMER, SUM(AMOUNT) FROM ORDERS GROUP BY CUSTOMER ORDER BY CUSTOMER"));
    }

    @Test
    void rejectsAggregatesOutsideTheSupportedSet() {
        assertThrows(UnsupportedOperationException.class,
            () -> run(Fixtures.salesFixture(), "SELECT AVG(SALES) FROM SALES"));
    }

    @Test
    void matchesCalciteOnCsvFixture() throws Exception {
        Catalog catalog = Catalog.load(List.of(Fixtures.path("sales.csv")));
        String sql = "SELECT REGION, SUM(SALES), MIN(SALES) FROM SALES WHERE REGION <> 'north' GROUP BY REGION ORDER BY REGION";

        List<List<Object>> expected;
        try (SqlEngine engine = new SqlEngine(catalog)) {
            expected = engine.query(sql);
        }
        assertEquals(expected, run(catalog, sql));
    }

    @Test
    void aggregatesParquetFileAcrossWorkers() throws Exception {
        Catalog catalog = Catalog.load(List.of(Fixtures.path("sales.parquet")));

        assertEquals(List.of(
            List.of("east", 19L),
            List.of("north", 11L),
            List.of("west", 28L)),
            run(catalog, "SELECT REGION, SUM(SALES) FROM SALES GROUP BY REGION ORDER BY REGION"));
    }

    @Test
    void selectStarScansEveryPartitionInItsOwnTask() throws Exception {
        Catalog catalog = Fixtures.salesFixture();
        java.io.PrintStream original = System.out;
        java.io.ByteArrayOutputStream captured = new java.io.ByteArrayOutputStream();
        List<List<Object>> rows;
        List<String> stages;
        System.setOut(new java.io.PrintStream(captured, true));
        try (RootAllocator allocator = new RootAllocator();
             Coordinator coordinator = new Coordinator(allocator, Partitioner.partition(catalog, WORKERS), WORKERS)) {
            rows = coordinator.execute(QueryPlanner.plan("SELECT * FROM SALES", catalog));
            stages = coordinator.stages();
        } finally {
            System.setOut(original);
        }

        assertEquals(8, rows.size());
        assertEquals("Stage 1: SCAN SALES x3", stages.get(0));
        String log = captured.toString();
        assertTrue(log.contains("worker-0: scan SALES -> 3 rows"), log);
        assertTrue(log.contains("worker-1: scan SALES -> 3 rows"), log);
        assertTrue(log.contains("worker-2: scan SALES -> 2 rows"), log);
    }
}
