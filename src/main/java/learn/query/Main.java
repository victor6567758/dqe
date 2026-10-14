package learn.query;

import learn.query.WorkloadManager.QueueConfig;
import learn.query.catalog.Catalog;
import learn.query.exec.LimitOperator;
import learn.query.exec.Pipeline;
import learn.query.sql.QueryPlanner;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class Main {

    private static final int WORKERS = 3;

    /**
     * Usage: one or more data files, then a SQL query.
     *   path/to/sales.csv "SELECT REGION, SUM(SALES) FROM SALES GROUP BY REGION"
     *   path/to/orders.csv path/to/sales.parquet "SQL"
     *
     * Each file becomes a table named after the file. The SQL is planned against those tables with
     * Calcite's rule-based planner, and the optimized plan runs on the distributed coordinator.
     */
    public static void main(String[] args) throws Exception {
        List<Path> files = new ArrayList<>();
        String sql = null;
        for (String arg : args) {
            if (isDataFile(arg)) {
                files.add(Path.of(arg));
            } else {
                sql = arg;
            }
        }
        if (files.isEmpty() || sql == null) {
            System.out.println("Usage: Main <data file>... \"<SQL query>\"");
            System.out.println("Each data file (.csv or .parquet) becomes a table named after the file.");
            return;
        }

        Catalog catalog = Catalog.load(files);
        List<Partition> partitions = Partitioner.partition(catalog, WORKERS);

        System.out.println("== Query ==");
        System.out.println(sql);

        System.out.println("== Optimized plan (HepPlanner, rule-based) ==");
        RelNode plan = QueryPlanner.plan(sql, catalog);
        System.out.print(RelOptUtil.toString(plan));

        System.out.println("== Distributed execution ==");
        QueueConfig queue = new QueueConfig("default", 2, 64L * 1024 * 1024,
            Duration.ofSeconds(5), Duration.ofSeconds(30));

        List<List<Object>> result;
        List<String> stages;
        try (RootAllocator allocator = new RootAllocator();
             Coordinator coordinator = new Coordinator(allocator, partitions, WORKERS, queue.jobMemoryBytes());
             WorkloadManager workloads = new WorkloadManager(queue)) {
            result = workloads.run(() -> coordinator.execute(plan));
            stages = coordinator.stages();
        }

        System.out.println("== Stages ==");
        stages.forEach(System.out::println);

        // Downstream operators consume the coordinator's rows. Here a limit caps the output.
        System.out.println("== Result ==");
        for (List<Object> row : Pipeline.of(new LimitOperator(100)).run(result)) {
            System.out.println(row);
        }
    }

    private static boolean isDataFile(String arg) {
        String lower = arg.toLowerCase(Locale.ROOT);
        return lower.endsWith(".csv") || lower.endsWith(".parquet");
    }
}
