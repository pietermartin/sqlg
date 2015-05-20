package org.umlg.sqlg.sql.dialect;

import org.postgresql.copy.CopyManager;
import org.umlg.sqlg.structure.SqlgGraph;

import java.io.InputStream;
import java.util.concurrent.Callable;

/**
 * Date: 2015/05/19
 * Time: 8:44 PM
 */
public class PostgresCopyInRunnable implements Callable<Long> {

    private SqlgGraph sqlgGraph;
    private InputStream inputStream;
    private String sql;
    private CopyManager copyManager;

    public PostgresCopyInRunnable(SqlgGraph sqlgGraph, CopyManager copyManager, String sql, InputStream inputStream) {
        this.sqlgGraph = sqlgGraph;
        this.copyManager = copyManager;
        this.sql = sql;
        this.inputStream = inputStream;
    }

    @Override
    public Long call() throws Exception {
        try {
            return this.copyManager.copyIn(this.sql, inputStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
