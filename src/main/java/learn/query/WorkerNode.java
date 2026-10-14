package learn.query;

import learn.query.exec.AggregateSpec;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Executor: runs one task at a time on behalf of the coordinator.
 * Each worker gets its own child allocator so memory can be tracked and limited per node.
 */
public class WorkerNode implements AutoCloseable {

    private final String id;
    private final BufferAllocator allocator;

    public WorkerNode(String id, BufferAllocator parent) {
        this(id, parent, Long.MAX_VALUE);
    }

    /** memoryLimitBytes caps the Arrow memory this node may allocate. Going over it throws OutOfMemoryException. */
    public WorkerNode(String id, BufferAllocator parent, long memoryLimitBytes) {
        this.id = id;
        this.allocator = parent.newChildAllocator(id, 0, memoryLimitBytes);
    }

    /** Scan task: reads this worker's partition of one table. */
    public List<Object[]> scan(Partition partition, String table) {
        List<Object[]> rows = partition.rows(table);
        System.out.println(id + ": scan " + table + " -> " + rows.size() + " rows");
        return rows;
    }

    /**
     * Partial aggregation over one partition's rows. Each state goes to the bucket that its
     * grouping key hashes to, and each bucket leaves as one Arrow batch.
     */
    public Map<Integer, VectorSchemaRoot> partialBuckets(List<Object[]> rows, AggregateSpec spec, int buckets) {
        Map<List<Object>, long[]> partial = spec.partialStates(rows);

        Map<Integer, Map<List<Object>, long[]>> byBucket = new HashMap<>();
        for (int b = 0; b < buckets; b++) {
            byBucket.put(b, new HashMap<>());
        }
        for (Map.Entry<List<Object>, long[]> entry : partial.entrySet()) {
            int bucket = Math.floorMod(entry.getKey().hashCode(), buckets);
            byBucket.get(bucket).put(entry.getKey(), entry.getValue());
        }

        Map<Integer, VectorSchemaRoot> out = new HashMap<>();
        for (int b = 0; b < buckets; b++) {
            out.put(b, ArrowBatches.fromPartials(allocator, spec.keyTypes(), byBucket.get(b), spec.calls().size()));
        }
        System.out.println(id + ": partial aggregate over " + rows.size() + " rows -> "
            + partial.size() + " groups");
        return out;
    }

    /** Merges the partial states of one bucket from every source, and produces the output rows. */
    public List<Object[]> mergeBucket(List<VectorSchemaRoot> inputs, AggregateSpec spec) {
        System.out.println(id + ": merging " + inputs.size() + " partial inputs");
        Map<List<Object>, long[]> merged = new TreeMap<>(AggregateSpec.KEY_ORDER);
        for (VectorSchemaRoot batch : inputs) {
            ArrowBatches.toPartials(batch, spec.keyTypes().size(), spec.calls().size())
                .forEach((key, state) -> merged.merge(key, state, spec::mergeStates));
        }
        return spec.finalRows(merged);
    }

    @Override
    public void close() {
        allocator.close();
    }
}
