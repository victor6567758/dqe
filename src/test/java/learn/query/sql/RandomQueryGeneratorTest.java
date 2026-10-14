package learn.query.sql;

import learn.query.sql.RandomQueryGenerator.GeneratedQuery;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RandomQueryGeneratorTest {

    @Test
    void sameSeedProducesSameQueries() {
        RandomQueryGenerator a = new RandomQueryGenerator(5);
        RandomQueryGenerator b = new RandomQueryGenerator(5);
        for (int i = 0; i < 50; i++) {
            assertEquals(a.next().sql(), b.next().sql());
        }
    }

    @Test
    void generatesVariedQueriesOverTheSalesTable() {
        RandomQueryGenerator generator = new RandomQueryGenerator(11);
        Set<String> distinct = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            GeneratedQuery query = generator.next();
            assertTrue(query.sql().startsWith("SELECT "));
            assertTrue(query.sql().contains(" FROM SALES"));
            distinct.add(query.sql());
        }
        assertTrue(distinct.size() > 50, "generator should produce varied queries");
    }
}
