package learn.query;

import learn.query.exec.AggregateSpec;
import learn.query.exec.RowOperators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Coordinator: divides an optimized plan into stages at its exchange points, runs each stage as
 * parallel tasks on the workers, and passes the results between stages through exchanges.
 *
 * The plan is walked from the leaves up. Every operator that needs data from other partitions
 * gets an exchange in front of it:
 *   SCAN: one task per partition reads that partition's rows of the table.
 *   FILTER, PROJECT: one task per partition, no data movement.
 *   JOIN: both inputs are repartitioned on their join keys, then each bucket is joined locally.
 *   AGGREGATE: partial states are hashed by group key, then each bucket is merged.
 *   SORT: the rows are gathered to the coordinator, then sorted.
 */
public class Coordinator implements AutoCloseable {

    /** Rows held between stages: one list per partition or per hash bucket. */
    public record Dataset(List<List<Object[]>> parts) {}

    private final List<Partition> partitions;
    private final List<WorkerNode> workers = new ArrayList<>();
    private final ExecutorService scheduler;
    // Runs the independent branches of a plan (the two inputs of a join) concurrently. Its threads
    // only wait for work, so they are kept apart from the worker pool and cannot starve it.
    private final ExecutorService branches = Executors.newCachedThreadPool();
    private final List<String> stages = Collections.synchronizedList(new ArrayList<>());

    public Coordinator(BufferAllocator allocator, List<Partition> partitions, int workerCount) {
        this(allocator, partitions, workerCount, Long.MAX_VALUE);
    }

    /** jobMemoryBytes limits the Arrow memory each worker may use for this coordinator's jobs. */
    public Coordinator(BufferAllocator allocator, List<Partition> partitions,
                       int workerCount, long jobMemoryBytes) {
        this.partitions = partitions;
        for (int i = 0; i < workerCount; i++) {
            workers.add(new WorkerNode("worker-" + i, allocator, jobMemoryBytes));
        }
        this.scheduler = Executors.newFixedThreadPool(workerCount);
    }

    /** Runs an optimized plan and returns one list of cell values per output row. */
    public List<List<Object>> execute(RelNode plan) throws Exception {
        stages.clear();
        Dataset result = exec(plan);
        return toLists(flatten(result.parts()));
    }

    /** The stages of the last execute call, in the order they ran. */
    public List<String> stages() {
        return List.copyOf(stages);
    }

    private Dataset exec(RelNode node) throws Exception {
        if (node instanceof TableScan scan) {
            return scan(scan);
        }
        if (node instanceof Filter filter) {
            Dataset in = exec(node.getInputs().get(0));
            return mapPartitions(in, "FILTER", part -> RowOperators.filter(filter.getCondition(), part));
        }
        if (node instanceof Project project) {
            Dataset in = exec(node.getInputs().get(0));
            return mapPartitions(in, "PROJECT", part -> RowOperators.project(project.getProjects(), part));
        }
        if (node instanceof Join join) {
            return join(join);
        }
        if (node instanceof Aggregate aggregate) {
            return aggregate(aggregate);
        }
        if (node instanceof Sort sort) {
            Dataset in = gather(exec(node.getInputs().get(0)));
            return mapPartitions(in, sort.fetch == null ? "SORT" : "SORT LIMIT", part -> RowOperators.sort(sort, part));
        }
        throw new UnsupportedOperationException("Operator not supported by the executor: " + node.getRelTypeName());
    }

    /** One task per partition. Each worker reads its own partition's rows, and the tasks run in parallel. */
    private Dataset scan(TableScan scan) throws Exception {
        String table = tableName(scan);
        List<Future<List<Object[]>>> tasks = new ArrayList<>();
        int index = 0;
        for (Partition partition : partitions) {
            WorkerNode worker = workers.get(index++ % workers.size());
            tasks.add(scheduler.submit(() -> worker.scan(partition, table)));
        }
        List<List<Object[]>> parts = Exchange.gather(tasks);
        stage("SCAN " + table + " x" + parts.size());
        return new Dataset(parts);
    }

    /** One task per partition. The operator runs where the data already is. */
    private Dataset mapPartitions(Dataset in, String label,
                                  Function<List<Object[]>, List<Object[]>> operator) throws Exception {
        List<Future<List<Object[]>>> tasks = new ArrayList<>();
        for (List<Object[]> part : in.parts()) {
            tasks.add(scheduler.submit(() -> operator.apply(part)));
        }
        List<List<Object[]>> out = Exchange.gather(tasks);
        stage(label + " x" + out.size());
        return new Dataset(out);
    }

    /** Gather exchange: every row moves to one place, the coordinator. */
    private Dataset gather(Dataset in) {
        List<Object[]> rows = flatten(in.parts());
        stage("GATHER " + in.parts().size() + " partitions to the coordinator");
        return new Dataset(List.of(rows));
    }

    /** Hash exchange: each row goes to the bucket that its key hashes to, using every worker. */
    private Dataset repartition(Dataset in, int[] keys) throws Exception {
        int buckets = workers.size();
        List<Future<List<List<Object[]>>>> tasks = new ArrayList<>();
        for (List<Object[]> part : in.parts()) {
            tasks.add(scheduler.submit(() -> bucketize(part, keys, buckets)));
        }
        List<List<List<Object[]>>> routed = Exchange.gather(tasks);

        List<List<Object[]>> out = new ArrayList<>();
        for (int b = 0; b < buckets; b++) {
            List<Object[]> bucket = new ArrayList<>();
            for (List<List<Object[]>> sourceRouting : routed) {
                bucket.addAll(sourceRouting.get(b));
            }
            out.add(bucket);
        }
        stage("HASH REPARTITION on " + Arrays.toString(keys) + " into " + buckets + " buckets");
        return new Dataset(out);
    }

    private Dataset join(Join join) throws Exception {
        if (join.getJoinType() != JoinRelType.INNER) {
            throw new UnsupportedOperationException("Only inner joins are supported: " + join.getJoinType());
        }
        // The two inputs are independent branches of the plan, so they run at the same time.
        Future<Dataset> leftBranch = branches.submit(() -> exec(join.getLeft()));
        Dataset right = exec(join.getRight());
        Dataset left = leftBranch.get();
        int[][] keys = equiKeys(join.getCondition(), join.getLeft().getRowType().getFieldCount());

        Dataset leftBuckets = repartition(left, keys[0]);
        Dataset rightBuckets = repartition(right, keys[1]);

        int buckets = leftBuckets.parts().size();
        List<Future<List<Object[]>>> tasks = new ArrayList<>();
        for (int b = 0; b < buckets; b++) {
            final int bucket = b;
            tasks.add(scheduler.submit(() -> hashJoin(
                leftBuckets.parts().get(bucket), rightBuckets.parts().get(bucket), keys[0], keys[1])));
        }
        List<List<Object[]>> out = Exchange.gather(tasks);
        stage("HASH JOIN x" + buckets);
        return new Dataset(out);
    }

    private Dataset aggregate(Aggregate aggregate) throws Exception {
        Dataset in = exec(aggregate.getInput());
        AggregateSpec spec = AggregateSpec.from(aggregate);
        // Without grouping keys there is nothing to hash on, so one bucket merges everything.
        int buckets = spec.keyTypes().isEmpty() ? 1 : workers.size();

        // Partial aggregation on every partition, with the states routed by group key.
        List<Future<Map<Integer, VectorSchemaRoot>>> partials = new ArrayList<>();
        int index = 0;
        for (List<Object[]> part : in.parts()) {
            WorkerNode worker = workers.get(index++ % workers.size());
            partials.add(scheduler.submit(() -> worker.partialBuckets(part, spec, buckets)));
        }
        List<Map<Integer, VectorSchemaRoot>> outputs = Exchange.gather(partials);
        try {
            stage("PARTIAL AGGREGATE x" + in.parts().size() + " -> HASH into " + buckets + " buckets");

            // Final merge, one task per bucket.
            List<Future<List<Object[]>>> finals = new ArrayList<>();
            for (int b = 0; b < buckets; b++) {
              List<VectorSchemaRoot> inputs = new ArrayList<>();
                for (Map<Integer, VectorSchemaRoot> output : outputs) {
                    inputs.add(output.get(b));
                }
                WorkerNode worker = workers.get(b % workers.size());
                finals.add(scheduler.submit(() -> worker.mergeBucket(inputs, spec)));
            }
            List<List<Object[]>> out = Exchange.gather(finals);
            stage("FINAL AGGREGATE x" + buckets);
            return new Dataset(out);
        } finally {
            outputs.forEach(output -> output.values().forEach(VectorSchemaRoot::close));
        }
    }

    /** Splits the join condition into key columns on the left and on the right. Only equalities are supported. */
    static int[][] equiKeys(RexNode condition, int leftCount) {
        List<Integer> leftKeys = new ArrayList<>();
        List<Integer> rightKeys = new ArrayList<>();
        for (RexNode conjunct : RelOptUtil.conjunctions(condition)) {
            if (!(conjunct instanceof RexCall)) {
                throw unsupportedCondition(conjunct);
            }
            RexCall call = (RexCall) conjunct;
            if (call.getKind() != SqlKind.EQUALS
                    || !(call.getOperands().get(0) instanceof RexInputRef)
                    || !(call.getOperands().get(1) instanceof RexInputRef)) {
                throw unsupportedCondition(conjunct);
            }
            int first = ((RexInputRef) call.getOperands().get(0)).getIndex();
            int second = ((RexInputRef) call.getOperands().get(1)).getIndex();
            if (first >= leftCount && second < leftCount) {
                int swap = first;
                first = second;
                second = swap;
            }
            if (first >= leftCount || second < leftCount) {
                throw unsupportedCondition(conjunct);
            }
            leftKeys.add(first);
            rightKeys.add(second - leftCount);
        }
        if (leftKeys.isEmpty()) {
            throw new UnsupportedOperationException("A join needs at least one equality condition");
        }
        return new int[][] {
            leftKeys.stream().mapToInt(Integer::intValue).toArray(),
            rightKeys.stream().mapToInt(Integer::intValue).toArray()};
    }

    /** Local hash join of one bucket. Rows with a null key never match, as in SQL. */
    static List<Object[]> hashJoin(List<Object[]> left, List<Object[]> right, int[] leftKeys, int[] rightKeys) {
        Map<List<Object>, List<Object[]>> index = new HashMap<>();
        for (Object[] row : right) {
            List<Object> key = keyOf(row, rightKeys);
            if (!key.contains(null)) {
                index.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }
        List<Object[]> out = new ArrayList<>();
        for (Object[] row : left) {
            List<Object> key = keyOf(row, leftKeys);
            if (key.contains(null)) {
                continue;
            }
            for (Object[] match : index.getOrDefault(key, List.of())) {
                out.add(joinRows(row, match));
            }
        }
        return out;
    }

    static List<List<Object[]>> bucketize(List<Object[]> rows, int[] keys, int buckets) {
        List<List<Object[]>> out = new ArrayList<>();
        for (int b = 0; b < buckets; b++) {
            out.add(new ArrayList<>());
        }
        for (Object[] row : rows) {
            out.get(Math.floorMod(keyOf(row, keys).hashCode(), buckets)).add(row);
        }
        return out;
    }

    private static List<Object> keyOf(Object[] row, int[] columns) {
        List<Object> key = new ArrayList<>(columns.length);
        for (int column : columns) {
            key.add(row[column]);
        }
        return key;
    }

    private static Object[] joinRows(Object[] left, Object[] right) {
        Object[] out = new Object[left.length + right.length];
        System.arraycopy(left, 0, out, 0, left.length);
        System.arraycopy(right, 0, out, left.length, right.length);
        return out;
    }

    private static UnsupportedOperationException unsupportedCondition(RexNode condition) {
        return new UnsupportedOperationException("Join conditions must be equalities between columns: " + condition);
    }

    private static List<Object[]> flatten(List<List<Object[]>> parts) {
        List<Object[]> rows = new ArrayList<>();
        parts.forEach(rows::addAll);
        return rows;
    }

    private static List<List<Object>> toLists(List<Object[]> rows) {
        List<List<Object>> out = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            out.add(new ArrayList<>(Arrays.asList(row)));
        }
        return out;
    }

    private static String tableName(TableScan scan) {
        List<String> names = scan.getTable().getQualifiedName();
        return names.get(names.size() - 1);
    }

    private synchronized void stage(String description) {
        stages.add("Stage " + (stages.size() + 1) + ": " + description);
    }

    @Override
    public void close() {
        branches.shutdownNow();
        scheduler.shutdown();
        workers.forEach(WorkerNode::close);
    }
}
