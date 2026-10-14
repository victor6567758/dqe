package learn.query.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;

import java.util.ArrayList;
import java.util.List;

/** Helpers for looking into an optimized plan. */
public final class PlanShape {

    private PlanShape() {}

    /** Returns the single Aggregate in the plan, or null if there is none. */
    public static Aggregate singleAggregate(RelNode node) {
        List<Aggregate> found = new ArrayList<>();
        collect(node, found);
        if (found.size() > 1) {
            throw new UnsupportedOperationException("Only one aggregate per query is supported");
        }
        return found.isEmpty() ? null : found.get(0);
    }

    private static void collect(RelNode node, List<Aggregate> found) {
        if (node instanceof Aggregate aggregate) {
            found.add(aggregate);
        }
        for (RelNode input : node.getInputs()) {
            collect(input, found);
        }
    }
}
