package learn.query.exec;

import java.util.ArrayList;
import java.util.List;

/** Sorts rows ascending by one column. Null values sort first. */
public final class SortOperator implements Operator {

    private final int column;

    public SortOperator(int column) {
        this.column = column;
    }

    @Override
    public List<List<Object>> apply(List<List<Object>> input) {
        List<List<Object>> out = new ArrayList<>(input);
        out.sort(this::compare);
        return out;
    }

    @SuppressWarnings("unchecked")
    private int compare(List<Object> x, List<Object> y) {
        Object a = x.get(column);
        Object b = y.get(column);
        if (a == null || b == null) {
            return a == null ? (b == null ? 0 : -1) : 1;
        }
        return ((Comparable<Object>) a).compareTo(b);
    }
}
