package learn.query.exec;

import java.util.ArrayList;
import java.util.List;

/**
 * Consumes rows from upstream and checks each one against an expected result.
 * Each output row is the input row with a status cell appended: OK or MISMATCH.
 * A final note row reports any difference in row count.
 */
public final class ReconcileOperator implements Operator {

    private final List<List<Object>> expected;

    public ReconcileOperator(List<List<Object>> expected) {
        this.expected = List.copyOf(expected);
    }

    @Override
    public List<List<Object>> apply(List<List<Object>> input) {
        List<List<Object>> out = new ArrayList<>();
        for (int i = 0; i < input.size(); i++) {
            List<Object> row = new ArrayList<>(input.get(i));
            boolean match = i < expected.size() && expected.get(i).equals(input.get(i));
            row.add(match ? "OK" : "MISMATCH");
            out.add(row);
        }
        if (input.size() != expected.size()) {
            List<Object> note = new ArrayList<>();
            note.add("row count: downstream=" + input.size() + ", expected=" + expected.size());
            out.add(note);
        }
        return out;
    }
}
