package learn.query.exec;

import java.util.ArrayList;
import java.util.List;

/** Keeps the given columns, in the given order. */
public final class ProjectOperator implements Operator {

    private final int[] columns;

    public ProjectOperator(int... columns) {
        this.columns = columns.clone();
    }

    @Override
    public List<List<Object>> apply(List<List<Object>> input) {
        List<List<Object>> out = new ArrayList<>(input.size());
        for (List<Object> row : input) {
            List<Object> projected = new ArrayList<>(columns.length);
            for (int column : columns) {
                projected.add(row.get(column));
            }
            out.add(projected);
        }
        return out;
    }
}
