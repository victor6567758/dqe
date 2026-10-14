package learn.query.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates random queries over the SALES table. Each query is returned as SQL text
 * together with its structure, so a reference evaluator can compute the expected result.
 */
public final class RandomQueryGenerator {

    public enum Agg { SUM, MIN, MAX, COUNT }

    /** A single WHERE condition: column is REGION or SALES; value is a String or Long. */
    public record Filter(String column, String op, Object value) {}

    public record GeneratedQuery(String sql, boolean grouped, List<Agg> aggregates, Filter filter) {}

    private static final String[] NUMERIC_OPS = {"=", "<>", "<", "<=", ">", ">="};
    private static final String[] REGIONS = {"east", "west", "north", "south"};

    private final Random random;

    public RandomQueryGenerator(long seed) {
        this.random = new Random(seed);
    }

    public GeneratedQuery next() {
        boolean grouped = random.nextBoolean();

        List<Agg> aggregates = new ArrayList<>();
        int count = 1 + random.nextInt(3);
        for (int i = 0; i < count; i++) {
            aggregates.add(Agg.values()[random.nextInt(Agg.values().length)]);
        }

        Filter filter = random.nextInt(3) == 0 ? null : randomFilter();

        List<String> select = new ArrayList<>();
        if (grouped) {
            select.add("REGION");
        }
        for (int i = 0; i < aggregates.size(); i++) {
            select.add(render(aggregates.get(i)) + " AS A" + i);
        }

        StringBuilder sql = new StringBuilder("SELECT ")
            .append(String.join(", ", select))
            .append(" FROM SALES");
        if (filter != null) {
            sql.append(" WHERE ").append(filter.column())
               .append(" ").append(filter.op())
               .append(" ").append(literal(filter.value()));
        }
        if (grouped) {
            sql.append(" GROUP BY REGION ORDER BY REGION");
        }
        return new GeneratedQuery(sql.toString(), grouped, aggregates, filter);
    }

    private Filter randomFilter() {
        if (random.nextBoolean()) {
            String op = NUMERIC_OPS[random.nextInt(NUMERIC_OPS.length)];
            long threshold = random.nextInt(111) - 10; // includes negative values
            return new Filter("SALES", op, threshold);
        }
        String op = random.nextBoolean() ? "=" : "<>";
        return new Filter("REGION", op, REGIONS[random.nextInt(REGIONS.length)]);
    }

    private static String render(Agg agg) {
        return agg == Agg.COUNT ? "COUNT(*)" : agg.name() + "(SALES)";
    }

    private static String literal(Object value) {
        return value instanceof String s ? "'" + s + "'" : value.toString();
    }
}
