package learn.query.exec;

import java.util.ArrayList;
import java.util.List;

/** Passes through at most the first n rows. */
public final class LimitOperator implements Operator {

    private final int limit;

    public LimitOperator(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must not be negative");
        }
        this.limit = limit;
    }

    @Override
    public List<List<Object>> apply(List<List<Object>> input) {
        return new ArrayList<>(input.subList(0, Math.min(limit, input.size())));
    }
}
