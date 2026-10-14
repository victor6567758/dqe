package learn.query;

import learn.query.catalog.Catalog;
import learn.query.sql.QueryPlanner;
import learn.query.sql.SqlEngine;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/** Multi-table queries with joins, aggregates, sorts, and limits, checked against Calcite and a hand-computed reference. */
class JoinTest {

    private static final int WORKERS = 3;
    private static Catalog WAREHOUSE;

    @BeforeAll
    static void loadTables() throws Exception {
        WAREHOUSE = Warehouse.catalog();
    }

    private static List<List<Object>> distributed(Catalog catalog, String sql) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
             Coordinator coordinator = new Coordinator(allocator, Partitioner.partition(catalog, WORKERS), WORKERS)) {
            return coordinator.execute(QueryPlanner.plan(sql, catalog));
        }
    }

    private static List<List<Object>> calcite(Catalog catalog, String sql) throws Exception {
        try (SqlEngine engine = new SqlEngine(catalog)) {
            return engine.query(sql);
        }
    }

    private static void assertSameAsCalcite(String sql) throws Exception {
        assertEquals(calcite(WAREHOUSE, sql), distributed(WAREHOUSE, sql), "Mismatch for: " + sql);
    }

    @Test
    void twoTableJoinWithGroupBy() throws Exception {
        assertSameAsCalcite(
            "SELECT c.COUNTRY, COUNT(*) AS N FROM CUSTOMERS c JOIN ORDERS o ON c.ID = o.CUSTOMER_ID "
                + "GROUP BY c.COUNTRY ORDER BY c.COUNTRY");
    }

    @Test
    void threeTableRevenueReportWithFilterAndHaving() throws Exception {
        assertSameAsCalcite(
            "SELECT c.COUNTRY, i.PRODUCT, SUM(i.QUANTITY * i.UNIT_PRICE) AS REVENUE "
                + "FROM CUSTOMERS c JOIN ORDERS o ON c.ID = o.CUSTOMER_ID "
                + "JOIN ITEMS i ON o.ID = i.ORDER_ID "
                + "WHERE o.STATUS = 'shipped' "
                + "GROUP BY c.COUNTRY, i.PRODUCT "
                + "HAVING SUM(i.QUANTITY * i.UNIT_PRICE) > 100 "
                + "ORDER BY c.COUNTRY, i.PRODUCT");
    }

    @Test
    void joinWithDescendingSortAndLimit() throws Exception {
        assertSameAsCalcite(
            "SELECT o.ID, c.NAME, o.AMOUNT FROM ORDERS o JOIN CUSTOMERS c ON o.CUSTOMER_ID = c.ID "
                + "WHERE o.AMOUNT > 150 ORDER BY o.AMOUNT DESC, o.ID LIMIT 5");
    }

    @Test
    void minMaxCountOverJoinDropsNullAndMissingKeys() throws Exception {
        assertSameAsCalcite(
            "SELECT c.COUNTRY, MIN(o.AMOUNT), MAX(o.AMOUNT), COUNT(*) FROM CUSTOMERS c "
                + "JOIN ORDERS o ON c.ID = o.CUSTOMER_ID GROUP BY c.COUNTRY ORDER BY c.COUNTRY");
    }

    @Test
    void selfJoinOnKey() throws Exception {
        assertSameAsCalcite(
            "SELECT a.COUNTRY, COUNT(*) AS N FROM CUSTOMERS a JOIN CUSTOMERS b ON a.ID = b.ID "
                + "GROUP BY a.COUNTRY ORDER BY a.COUNTRY");
    }

    @Test
    void revenueByCountryMatchesHandComputation() throws Exception {
        Map<Long, String> countryOf = new HashMap<>();
        for (Object[] customer : WAREHOUSE.get("CUSTOMERS").rows()) {
            countryOf.put((Long) customer[0], (String) customer[2]);
        }
        Map<Long, Object[]> orderById = new HashMap<>();
        for (Object[] order : WAREHOUSE.get("ORDERS").rows()) {
            orderById.put((Long) order[0], order);
        }

        Map<String, Long> revenue = new TreeMap<>();
        for (Object[] item : WAREHOUSE.get("ITEMS").rows()) {
            Object[] order = orderById.get((Long) item[0]);
            if (order == null || order[1] == null || !"shipped".equals(order[3])) {
                continue;
            }
            String country = countryOf.get((Long) order[1]);
            if (country != null) {
                revenue.merge(country, (Long) item[2] * (Long) item[3], Long::sum);
            }
        }

        List<List<Object>> expected = new ArrayList<>();
        revenue.forEach((country, total) -> expected.add(List.<Object>of(country, total)));

        assertEquals(expected, distributed(WAREHOUSE,
            "SELECT c.COUNTRY, SUM(i.QUANTITY * i.UNIT_PRICE) AS REVENUE FROM CUSTOMERS c "
                + "JOIN ORDERS o ON c.ID = o.CUSTOMER_ID JOIN ITEMS i ON o.ID = i.ORDER_ID "
                + "WHERE o.STATUS = 'shipped' GROUP BY c.COUNTRY ORDER BY c.COUNTRY"));
    }

    @Test
    void joinStagesRepartitionBothInputsAndMergeTheResult() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
             Coordinator coordinator = new Coordinator(allocator, Partitioner.partition(WAREHOUSE, WORKERS), WORKERS)) {
            coordinator.execute(QueryPlanner.plan(
                "SELECT c.COUNTRY, COUNT(*) FROM CUSTOMERS c JOIN ORDERS o ON c.ID = o.CUSTOMER_ID "
                    + "GROUP BY c.COUNTRY ORDER BY c.COUNTRY", WAREHOUSE));

            String stages = String.join("\n", coordinator.stages());
            assertTrue(stages.contains("HASH REPARTITION"), stages);
            assertTrue(stages.contains("HASH JOIN"), stages);
            assertTrue(stages.contains("PARTIAL AGGREGATE"), stages);
            assertTrue(stages.contains("FINAL AGGREGATE"), stages);
            assertTrue(stages.contains("GATHER"), stages);
        }
    }

    @Test
    void joinOfTwoAggregatedBranchesRunsBothBranches() throws Exception {
        // Each input is its own aggregate over the whole table: independent branches of the plan.
        assertSameAsCalcite(
            "SELECT a.COUNTRY, a.N, b.REVENUE FROM "
                + "(SELECT COUNTRY, COUNT(*) AS N FROM CUSTOMERS GROUP BY COUNTRY) a "
                + "JOIN (SELECT c.COUNTRY, SUM(o.AMOUNT) AS REVENUE FROM CUSTOMERS c "
                + "JOIN ORDERS o ON c.ID = o.CUSTOMER_ID GROUP BY c.COUNTRY) b "
                + "ON a.COUNTRY = b.COUNTRY ORDER BY a.COUNTRY");
    }

    @Test
    void residualJoinConditionsAreRejected() {
        assertThrows(UnsupportedOperationException.class, () -> distributed(WAREHOUSE,
            "SELECT COUNT(*) FROM CUSTOMERS a JOIN CUSTOMERS b ON a.COUNTRY = b.COUNTRY AND a.ID < b.ID"));
    }
}
