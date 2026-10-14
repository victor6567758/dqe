package learn.query.sql;

import learn.query.sql.RandomQueryGenerator.Agg;
import learn.query.sql.RandomQueryGenerator.Filter;
import learn.query.sql.RandomQueryGenerator.GeneratedQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Computes the expected result of a generated query in plain Java, with no SQL engine.
 * Rows are {region, sales} in the SALES table's column order. Results are rendered as strings
 * so they can be compared with engine output directly.
 * Follows SQL semantics: SUM, MIN, and MAX over no rows are NULL, COUNT is 0,
 * and an ungrouped aggregate always returns exactly one row.
 */
public final class ReferenceEvaluator {

    private ReferenceEvaluator() {}

    public static List<List<String>> evaluate(GeneratedQuery query, List<Object[]> rows) {
        List<Object[]> kept = new ArrayList<>();
        for (Object[] row : rows) {
            if (matches(query.filter(), row)) {
                kept.add(row);
            }
        }

        List<List<String>> out = new ArrayList<>();
        if (query.grouped()) {
            Map<String, List<Object[]>> groups = new TreeMap<>();
            for (Object[] row : kept) {
                groups.computeIfAbsent((String) row[0], k -> new ArrayList<>()).add(row);
            }
            groups.forEach((region, members) -> {
                List<String> cells = new ArrayList<>();
                cells.add(region);
                cells.addAll(aggregates(query.aggregates(), members));
                out.add(cells);
            });
        } else {
            out.add(aggregates(query.aggregates(), kept));
        }
        return out;
    }

    private static List<String> aggregates(List<Agg> aggs, List<Object[]> members) {
        List<String> cells = new ArrayList<>();
        for (Agg agg : aggs) {
            cells.add(String.valueOf(compute(agg, members)));
        }
        return cells;
    }

    private static Object compute(Agg agg, List<Object[]> members) {
        if (agg == Agg.COUNT) {
            return (long) members.size();
        }
        if (members.isEmpty()) {
            return null;
        }
        long result = agg == Agg.SUM ? 0L : (Long) members.get(0)[1];
        for (Object[] row : members) {
            long v = (Long) row[1];
            result = switch (agg) {
                case SUM -> result + v;
                case MIN -> Math.min(result, v);
                case MAX -> Math.max(result, v);
                case COUNT -> result;
            };
        }
        return result;
    }

    static boolean matches(Filter filter, Object[] row) {
        if (filter == null) {
            return true;
        }
        if (filter.column().equals("REGION")) {
            boolean equal = row[0].equals(filter.value());
            return filter.op().equals("=") ? equal : !equal;
        }
        long value = (Long) row[1];
        long threshold = (Long) filter.value();
        return switch (filter.op()) {
            case "=" -> value == threshold;
            case "<>" -> value != threshold;
            case "<" -> value < threshold;
            case "<=" -> value <= threshold;
            case ">" -> value > threshold;
            case ">=" -> value >= threshold;
            default -> throw new IllegalArgumentException(filter.op());
        };
    }
}
