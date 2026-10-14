package learn.query.sql;

import learn.query.Fixtures;
import learn.query.catalog.Catalog;
import learn.query.catalog.Column;
import learn.query.catalog.Table;
import learn.query.exec.PlanShape;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryPlannerTest {

    private static final Catalog SALES = Fixtures.salesCatalog(List.of());

    @Test
    void groupedQueryPlansOneAggregate() {
        RelNode plan = QueryPlanner.plan("SELECT REGION, SUM(SALES) FROM SALES GROUP BY REGION", SALES);
        assertNotNull(PlanShape.singleAggregate(plan));
    }

    @Test
    void planningUsesTheCatalogNotAFixedSchema() {
        Catalog orders = Catalog.of(new Table("ORDERS",
            List.of(new Column("CUSTOMER", SqlTypeName.VARCHAR), new Column("AMOUNT", SqlTypeName.BIGINT)),
            List.of()));
        RelNode plan = QueryPlanner.plan("SELECT CUSTOMER, COUNT(*) FROM ORDERS GROUP BY CUSTOMER", orders);
        assertNotNull(PlanShape.singleAggregate(plan));
    }

    @Test
    void rejectsTablesMissingFromTheCatalog() {
        assertThrows(IllegalArgumentException.class,
            () -> QueryPlanner.plan("SELECT COUNT(*) FROM ORDERS", SALES));
    }

    @Test
    void rejectsUnknownColumns() {
        assertThrows(IllegalArgumentException.class,
            () -> QueryPlanner.plan("SELECT NOPE FROM SALES", SALES));
    }

    @Test
    void rejectsSyntaxErrors() {
        assertThrows(IllegalArgumentException.class,
            () -> QueryPlanner.plan("SELECT FROM WHERE", SALES));
    }
}
