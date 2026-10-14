package learn.query.catalog;

import org.apache.calcite.sql.type.SqlTypeName;

/** One column of a table. The skeleton supports VARCHAR and BIGINT columns. */
public record Column(String name, SqlTypeName type) {

    public Column {
        if (type != SqlTypeName.VARCHAR && type != SqlTypeName.BIGINT) {
            throw new IllegalArgumentException("Unsupported type " + type + " for column " + name);
        }
    }
}
