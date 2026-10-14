package learn.query.exec;

import java.util.ArrayList;
import java.util.List;

/** Runs a source of rows through a chain of operators, each consuming the previous stage's output. */
public final class Pipeline {

    private final List<Operator> operators;

    private Pipeline(List<Operator> operators) {
        this.operators = List.copyOf(operators);
    }

    public static Pipeline of(Operator... operators) {
        return new Pipeline(List.of(operators));
    }

    public List<List<Object>> run(List<List<Object>> source) {
        List<List<Object>> current = new ArrayList<>(source);
        for (Operator operator : operators) {
            current = operator.apply(current);
        }
        return current;
    }
}
