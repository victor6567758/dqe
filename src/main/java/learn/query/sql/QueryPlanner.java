package learn.query.sql;

import learn.query.catalog.Catalog;
import learn.query.catalog.Table;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.util.List;

/**
 * Builds the logical plan for a SQL statement against a catalog.
 *
 * Calcite parses and validates the statement against the catalog's tables, converts it into a
 * tree of relational operators, and a HepPlanner applies a fixed program of rewrite rules until
 * none of them matches. This is rule-based planning. No cost model is used.
 */
public final class QueryPlanner {

    private QueryPlanner() {}

    public static RelNode plan(String sql, Catalog catalog) {
        SchemaPlus schema = Frameworks.createRootSchema(true);
        for (Table table : catalog.tables()) {
            schema.add(table.name(), new CatalogTable(table));
        }
        FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(schema).build();

        RelNode logical;
        try (Planner planner = Frameworks.getPlanner(config)) {
            SqlNode parsed = planner.parse(sql);
            SqlNode validated = planner.validate(parsed);
            logical = planner.rel(validated).project();
        } catch (SqlParseException | ValidationException | RelConversionException | RuntimeException e) {
            throw new IllegalArgumentException("Cannot plan SQL: " + e.getMessage(), e);
        }

        HepProgram program = new HepProgramBuilder()
            .addRuleCollection(List.of(
                CoreRules.FILTER_MERGE,
                CoreRules.PROJECT_MERGE,
                CoreRules.PROJECT_REMOVE))
            .build();
        HepPlanner hep = new HepPlanner(program);
        hep.setRoot(logical);
        return hep.findBestExp();
    }
}
