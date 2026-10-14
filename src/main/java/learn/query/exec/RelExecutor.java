package learn.query.exec;

import learn.query.Partition;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

/** Runs plan operators: local subtrees on workers, and operators above the exchange on the coordinator. */
public final class RelExecutor {

    private RelExecutor() {}

    /** Runs a local subtree (scan, filter, projection) over one partition. Runs on workers. */
    public static List<Object[]> evaluateLocal(RelNode node, Partition partition) {
        if (node instanceof TableScan scan) {
            return partition.rows(tableName(scan));
        }
        List<Object[]> input = evaluateLocal(single(node), partition);
        if (node instanceof Filter filter) {
            return RowOperators.filter(filter.getCondition(), input);
        }
        if (node instanceof Project project) {
            return RowOperators.project(project.getProjects(), input);
        }
        throw new UnsupportedOperationException("Not a local operator: " + node.getRelTypeName());
    }

    /** Runs the operators between the exchange and the root of the plan. Runs on the coordinator. */
    public static List<Object[]> applyAbove(RelNode node, RelNode exchange, List<Object[]> exchanged) {
        if (node == exchange) {
            return exchanged;
        }
        List<Object[]> input = applyAbove(single(node), exchange, exchanged);
        if (node instanceof Filter filter) {
            return RowOperators.filter(filter.getCondition(), input);
        }
        if (node instanceof Project project) {
            return RowOperators.project(project.getProjects(), input);
        }
        if (node instanceof Sort sort) {
            return RowOperators.sort(sort, input);
        }
        throw new UnsupportedOperationException("Operator above the exchange is not supported: " + node.getRelTypeName());
    }

    static RelNode single(RelNode node) {
        if (node.getInputs().size() != 1) {
            throw new UnsupportedOperationException("Only single-input operators are supported: " + node.getRelTypeName());
        }
        return node.getInputs().get(0);
    }

    private static String tableName(TableScan scan) {
        List<String> names = scan.getTable().getQualifiedName();
        return names.get(names.size() - 1);
    }
}
