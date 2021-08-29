package org.umlg.sqlg.ui;

import org.umlg.sqlg.structure.SqlgGraph;
import spark.Spark;

public class SqlgUI {

    private final SqlgGraph sqlgGraph;
    public static SqlgUI INSTANCE = null;

    public static void get(SqlgGraph sqlgGraph) {
        if (INSTANCE == null) {
            SparkResources.resources();
            Spark.awaitInitialization();
            INSTANCE = new SqlgUI(sqlgGraph);
        }
    }

    public static void stop() {
        INSTANCE = null;
        Spark.stop();
    }

    private SqlgUI(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
    }

    public SqlgGraph getSqlgGraph() {
        return sqlgGraph;
    }
}
