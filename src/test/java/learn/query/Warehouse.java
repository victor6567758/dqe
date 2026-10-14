package learn.query;

import learn.query.catalog.Catalog;

import java.util.List;

/**
 * The three-table join dataset, loaded from fixture files in src/test/resources:
 * customers.csv, orders.csv, and items.parquet. scripts/make_fixtures.py generates them.
 * The two formats together show that tables from different files join the same way.
 */
public final class Warehouse {

    private Warehouse() {}

    public static Catalog catalog() throws Exception {
        return Catalog.load(List.of(
            Fixtures.path("customers.csv"),
            Fixtures.path("orders.csv"),
            Fixtures.path("items.parquet")));
    }
}
