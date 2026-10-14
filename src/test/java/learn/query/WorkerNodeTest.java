package learn.query;

import learn.query.exec.AggregateSpec;
import learn.query.exec.PlanShape;
import learn.query.sql.QueryPlanner;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WorkerNodeTest {

    private static final String SQL = "SELECT REGION, SUM(SALES) FROM SALES GROUP BY REGION";

    private static Aggregate aggregate() {
        return PlanShape.singleAggregate(QueryPlanner.plan(SQL, Fixtures.salesCatalog(List.<Object[]>of(new Object[] {"east", 10L}))));
    }

    private static List<Object[]> oneRow() {
        return List.<Object[]>of(new Object[] {"east", 10L});
    }

    @Test
    void partialAggregateFitsWithinItsMemoryBudget() {
        Aggregate agg = aggregate();
        try (RootAllocator allocator = new RootAllocator();
             WorkerNode worker = new WorkerNode("roomy", allocator, 64L * 1024 * 1024)) {
            VectorSchemaRoot root = worker.partialBuckets(oneRow(), AggregateSpec.from(agg), 1).get(0);
            assertEquals(1, root.getRowCount());
            root.close();
        }
    }

    @Test
    void rejectsPartialAggregateThatExceedsItsMemoryBudget() {
        Aggregate agg = aggregate();
        try (RootAllocator allocator = new RootAllocator();
             WorkerNode worker = new WorkerNode("tiny", allocator, 16)) {
            assertThrows(OutOfMemoryException.class,
                () -> worker.partialBuckets(oneRow(), AggregateSpec.from(agg), 1));
        }
    }
}
