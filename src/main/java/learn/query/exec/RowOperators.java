package learn.query.exec;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/** Filter, projection, and sort over lists of rows. */
public final class RowOperators {

    private RowOperators() {}

    public static List<Object[]> filter(RexNode condition, List<Object[]> input) {
        List<Object[]> out = new ArrayList<>();
        for (Object[] row : input) {
            if (RexEvaluator.test(condition, row)) {
                out.add(row);
            }
        }
        return out;
    }

    public static List<Object[]> project(List<RexNode> expressions, List<Object[]> input) {
        List<Object[]> out = new ArrayList<>(input.size());
        for (Object[] row : input) {
            Object[] projected = new Object[expressions.size()];
            for (int i = 0; i < expressions.size(); i++) {
                projected[i] = RexEvaluator.eval(expressions.get(i), row);
            }
            out.add(projected);
        }
        return out;
    }

    public static List<Object[]> sort(Sort sort, List<Object[]> input) {
        if (sort.offset != null) {
            throw new UnsupportedOperationException("OFFSET is not supported");
        }
        List<RelFieldCollation> fields = sort.getCollation().getFieldCollations();
        List<Object[]> out = new ArrayList<>(input);
        out.sort((x, y) -> {
            for (RelFieldCollation field : fields) {
                int c = compareNullsFirst(x[field.getFieldIndex()], y[field.getFieldIndex()]);
                if (field.getDirection() == RelFieldCollation.Direction.DESCENDING) {
                    c = -c;
                }
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        });
        if (sort.fetch != null) {
            int limit = RexLiteral.intValue(sort.fetch);
            if (out.size() > limit) {
                return new ArrayList<>(out.subList(0, limit));
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static int compareNullsFirst(Object a, Object b) {
        if (a == null || b == null) {
            return a == null ? (b == null ? 0 : -1) : 1;
        }
        return ((Comparable<Object>) a).compareTo(b);
    }
}
