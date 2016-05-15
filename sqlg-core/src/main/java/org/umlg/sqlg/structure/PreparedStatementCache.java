package org.umlg.sqlg.structure;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2016/05/15
 * Time: 2:24 PM
 */
public class PreparedStatementCache {

    private List<PreparedStatement> cache = new ArrayList<>();

    void add(PreparedStatement preparedStatement) {
        this.cache.add(preparedStatement);
    }

    public void close() throws SQLException {
        for (PreparedStatement preparedStatement : this.cache) {
            preparedStatement.close();
        }
        this.cache.clear();
    }

    public boolean isEmpty() {
        return this.cache.isEmpty();
    }
}
