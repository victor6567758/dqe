package learn.query.sql;

import learn.query.catalog.Column;
import learn.query.catalog.Table;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/** Exposes a catalog table to Calcite. The row type comes from the table's column definitions. */
public final class CatalogTable extends AbstractTable implements ScannableTable {

    private final Table table;

    public CatalogTable(Table table) {
        this.table = table;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Column column : table.columns()) {
            // Every column is nullable: empty CSV values and null Parquet values load as NULL.
            RelDataType nullable = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(column.type()), true);
            builder.add(column.name(), nullable);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(table.rows());
    }
}
