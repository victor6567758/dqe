package learn.query.exec;

import java.util.List;

/**
 * A downstream operator: consumes rows produced upstream and produces rows for the next stage.
 * Rows are lists of cell values. Operators are chained with Pipeline.
 */
public interface Operator {
    List<List<Object>> apply(List<List<Object>> input);
}
