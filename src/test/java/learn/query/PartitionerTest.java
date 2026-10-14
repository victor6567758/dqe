package learn.query;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PartitionerTest {

    @Test
    void splitsIntoContiguousChunksWithoutLosingRows() {
        List<Integer> rows = List.of(1, 2, 3, 4, 5, 6, 7, 8);

        List<List<Integer>> parts = Partitioner.split(rows, 3);

        assertEquals(3, parts.size());
        assertEquals(List.of(1, 2, 3), parts.get(0));
        assertEquals(List.of(7, 8), parts.get(2));

        List<Integer> recombined = new ArrayList<>();
        parts.forEach(recombined::addAll);
        assertEquals(rows, recombined);
    }

    @Test
    void handlesMorePartitionsThanRows() {
        List<List<String>> parts = Partitioner.split(List.of("a", "b"), 5);

        assertEquals(2, parts.size());
        assertTrue(parts.stream().allMatch(p -> p.size() == 1));
    }

    @Test
    void handlesEmptyInput() {
        assertTrue(Partitioner.split(List.<String>of(), 3).isEmpty());
    }

    @Test
    void rejectsZeroPartitions() {
        assertThrows(IllegalArgumentException.class, () -> Partitioner.split(List.of(1), 0));
    }
}
