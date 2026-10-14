package learn.query.exec;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OperatorsTest {

    private static final List<List<Object>> ROWS = List.of(
        List.of("north", 11L),
        List.of("east", 19L),
        List.of("west", 28L));

    @Test
    void projectKeepsRequestedColumnsInOrder() {
        List<List<Object>> out = new ProjectOperator(1, 0).apply(ROWS);
        assertEquals(List.of(List.of(11L, "north"), List.of(19L, "east"), List.of(28L, "west")), out);
    }

    @Test
    void sortOrdersByColumnWithNullsFirst() {
        List<List<Object>> input = Arrays.asList(
            List.of("west", 28L),
            Arrays.asList(null, 0L),
            List.of("east", 19L));
        List<List<Object>> out = new SortOperator(0).apply(input);
        assertNull(out.get(0).get(0));
        assertEquals("east", out.get(1).get(0));
        assertEquals("west", out.get(2).get(0));
    }

    @Test
    void limitTakesAPrefix() {
        assertEquals(2, new LimitOperator(2).apply(ROWS).size());
        assertEquals(3, new LimitOperator(10).apply(ROWS).size());
    }

    @Test
    void reconcileMarksMatchesAndMismatches() {
        List<List<Object>> expected = List.of(List.of("north", 11L), List.of("east", 20L));
        List<List<Object>> out = new ReconcileOperator(expected).apply(ROWS.subList(0, 2));

        assertEquals("OK", out.get(0).get(2));
        assertEquals("MISMATCH", out.get(1).get(2));
    }

    @Test
    void reconcileReportsRowCountDifference() {
        List<List<Object>> out = new ReconcileOperator(List.of(List.of("north", 11L))).apply(ROWS);
        assertEquals(4, out.size());
        assertTrue(out.get(3).get(0).toString().startsWith("row count"));
    }

    @Test
    void pipelineRunsOperatorsInOrder() {
        List<List<Object>> out = Pipeline.of(new SortOperator(1), new LimitOperator(1)).run(ROWS);
        assertEquals(List.of(List.of("north", 11L)), out);
    }
}
