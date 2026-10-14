package learn.query.sql;

import learn.query.Coordinator;
import learn.query.Fixtures;
import learn.query.Partition;
import learn.query.Partitioner;
import learn.query.catalog.Catalog;
import learn.query.sql.RandomQueryGenerator.GeneratedQuery;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Differential testing across execution paths: each random query runs through Calcite's engine
 * on one node and through the distributed coordinator, and the results must match.
 */
class CoordinatorRandomQueryTest {

    private static final String[] REGIONS = {"east", "west", "north", "south"};

    @Test
    void coordinatorMatchesCalciteOnRandomQueries() throws Exception {
        Random random = new Random(7);
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            rows.add(new Object[] {REGIONS[random.nextInt(REGIONS.length)], (long) random.nextInt(100)});
        }
        Catalog catalog = Fixtures.salesCatalog(rows);
        List<Partition> partitions = Partitioner.partition(catalog, 3);
        RandomQueryGenerator generator = new RandomQueryGenerator(2024);

        try (SqlEngine engine = new SqlEngine(catalog);
             RootAllocator allocator = new RootAllocator();
             Coordinator coordinator = new Coordinator(allocator, partitions, 3)) {
            for (int i = 0; i < 200; i++) {
                GeneratedQuery query = generator.next();
                List<List<String>> expected = stringify(engine.query(query.sql()));
                List<List<String>> actual = stringify(
                    coordinator.execute(QueryPlanner.plan(query.sql(), catalog)));
                assertEquals(expected, actual, "Mismatch for query: " + query.sql());
            }
        }
    }

    private static List<List<String>> stringify(List<List<Object>> rows) {
        List<List<String>> out = new ArrayList<>();
        for (List<Object> row : rows) {
            List<String> cells = new ArrayList<>();
            for (Object value : row) {
                cells.add(String.valueOf(value));
            }
            out.add(cells);
        }
        return out;
    }
}
