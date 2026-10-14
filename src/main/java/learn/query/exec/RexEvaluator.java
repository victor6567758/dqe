package learn.query.exec;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.List;

/**
 * Evaluates Calcite row expressions over Object[] rows.
 * Supports column references, literals, comparisons, AND, OR, and NOT, with SQL null semantics.
 */
public final class RexEvaluator {

    private RexEvaluator() {}

    public static Object eval(RexNode node, Object[] row) {
        if (node instanceof RexInputRef ref) {
            return row[ref.getIndex()];
        }
        if (node instanceof RexLiteral literal) {
            return literalValue(literal);
        }
        if (node instanceof RexCall call) {
            return call(call, row);
        }
        throw new UnsupportedOperationException("Unsupported expression: " + node);
    }

    /** True only when the predicate evaluates to TRUE. FALSE and unknown (null) both reject the row. */
    public static boolean test(RexNode predicate, Object[] row) {
        return Boolean.TRUE.equals(eval(predicate, row));
    }

    private static Object literalValue(RexLiteral literal) {
        if (literal.isNull()) {
            return null;
        }
        Object value = literal.getValue();
        if (value instanceof NlsString s) {
            return s.getValue();
        }
        if (value instanceof BigDecimal d) {
            return d.longValueExact();
        }
        if (value instanceof Boolean b) {
            return b;
        }
        throw new UnsupportedOperationException("Unsupported literal: " + literal);
    }

    private static Object call(RexCall call, Object[] row) {
        List<RexNode> operands = call.getOperands();
        SqlKind kind = call.getKind();
        switch (kind) {
            case AND: {
                Object result = Boolean.TRUE;
                for (RexNode operand : operands) {
                    Object v = eval(operand, row);
                    if (Boolean.FALSE.equals(v)) {
                        return Boolean.FALSE;
                    }
                    if (v == null) {
                        result = null;
                    }
                }
                return result;
            }
            case OR: {
                Object result = Boolean.FALSE;
                for (RexNode operand : operands) {
                    Object v = eval(operand, row);
                    if (Boolean.TRUE.equals(v)) {
                        return Boolean.TRUE;
                    }
                    if (v == null) {
                        result = null;
                    }
                }
                return result;
            }
            case NOT: {
                Object v = eval(operands.get(0), row);
                return v == null ? null : !((Boolean) v);
            }
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return compare(kind, eval(operands.get(0), row), eval(operands.get(1), row));
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
                return arithmetic(kind, eval(operands.get(0), row), eval(operands.get(1), row));
            case CAST:
                // The planner has already matched the types (BIGINT to BIGINT, VARCHAR to VARCHAR).
                return eval(operands.get(0), row);
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + call.getOperator().getName());
        }
    }

    private static Object arithmetic(SqlKind kind, Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        long x = (Long) a;
        long y = (Long) b;
        return switch (kind) {
            case PLUS -> x + y;
            case MINUS -> x - y;
            case TIMES -> x * y;
            case DIVIDE -> x / y;
            default -> throw new IllegalStateException(kind.name());
        };
    }

    @SuppressWarnings("unchecked")
    private static Object compare(SqlKind kind, Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        int c = ((Comparable<Object>) a).compareTo(b);
        return switch (kind) {
            case EQUALS -> c == 0;
            case NOT_EQUALS -> c != 0;
            case LESS_THAN -> c < 0;
            case LESS_THAN_OR_EQUAL -> c <= 0;
            case GREATER_THAN -> c > 0;
            case GREATER_THAN_OR_EQUAL -> c >= 0;
            default -> throw new IllegalStateException(kind.name());
        };
    }
}
