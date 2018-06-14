package org.umlg.sqlg.structure;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Cache all statements to close them when iteration is done
 * Date: 2016/05/15
 * Time: 2:24 PM
 */
public class PreparedStatementCache {

    private Map<PreparedStatement,Boolean> cache = new IdentityHashMap<>();

    void add(PreparedStatement preparedStatement) {
        this.cache.put(preparedStatement,Boolean.TRUE);
    }
    
    void remove(PreparedStatement preparedStatement) {
        this.cache.remove(preparedStatement);
    }
    
    public void close() throws SQLException {
        for (PreparedStatement preparedStatement : this.cache.keySet()) {
            preparedStatement.close();
        }
        this.cache.clear();
    }

    public boolean isEmpty() {
        return this.cache.isEmpty();
    }
    
    public int size() {
    	return this.cache.size();
    }
}
