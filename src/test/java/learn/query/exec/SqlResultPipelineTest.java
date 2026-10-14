package learn.query.exec;

import learn.query.Coordinator;
import learn.query.Fixtures;
import learn.query.Partitioner;
import learn.query.catalog.Catalog;
import learn.query.sql.QueryPlanner;
import learn.query.sql.SqlEngine;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The distributed result flows into downstream operators, which check it against Calcite's engine. */
class SqlResultPipelineTest {

    private static final String SQL =
        "SELECT REGION, SUM(SALES) AS TOTAL_SALES FROM SALES GROUP BY REGION ORDER BY REGION";

    @Test
    void distributedResultReconcilesWithCalcite() throws Exception {
        Catalog catalog = Fixtures.salesFixture();

        List<List<Object>> calciteRows;
        try (SqlEngine engine = new SqlEngine(catalog)) {
            calciteRows = engine.query(SQL);
        }

        RelNode plan = QueryPlanner.plan(SQL, catalog);
        List<List<Object>> distributedRows;
        try (RootAllocator allocator = new RootAllocator();
             Coordinator coordinator = new Coordinator(allocator, Partitioner.partition(catalog, 3), 3)) {
            distributedRows = coordinator.execute(plan);
        }

        List<List<Object>> report = Pipeline.of(
            new ReconcileOperator(calciteRows),
            new SortOperator(0)).run(distributedRows);

        assertEquals(3, report.size());
        for (List<Object> row : report) {
            assertEquals("OK", row.get(row.size() - 1), "row did not reconcile: " + row);
        }
    }
}
