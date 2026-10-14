package learn.query.sql;

import learn.query.Fixtures;
import learn.query.sql.RandomQueryGenerator.GeneratedQuery;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Differential testing: every random query runs through Calcite's engine and through the
 * plain-Java reference evaluator, and the two results must match.
 */
class RandomQueryTest {

    private static final int QUERIES = 300;
    private static final String[] REGIONS = {"east", "west", "north", "south"};

    @Test
    void calciteMatchesReferenceOnRandomQueries() throws Exception {
        assertMatchesReference(randomRows(new Random(7), 500), 42);
    }

    @Test
    void calciteMatchesReferenceOnEmptyTable() throws Exception {
        assertMatchesReference(List.of(), 99);
    }

    @Test
    void calciteMatchesReferenceOnSmallTable() throws Exception {
        List<Object[]> rows = List.of(
            new Object[] {"east", 0L},
            new Object[] {"west", -5L},
            new Object[] {"east", 10L});
        assertMatchesReference(rows, 123);
    }

    private static void assertMatchesReference(List<Object[]> rows, long seed) throws Exception {
        RandomQueryGenerator generator = new RandomQueryGenerator(seed);
        try (SqlEngine engine = new SqlEngine(Fixtures.salesCatalog(rows))) {
            for (int i = 0; i < QUERIES; i++) {
                GeneratedQuery query = generator.next();
                List<List<String>> expected = ReferenceEvaluator.evaluate(query, rows);
                List<List<String>> actual = stringify(engine.query(query.sql()));
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

    private static List<Object[]> randomRows(Random random, int count) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(new Object[] {REGIONS[random.nextInt(REGIONS.length)], (long) random.nextInt(100)});
        }
        return rows;
    }
}
