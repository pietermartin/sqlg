package org.umlg.sqlg.process.step.map;

import com.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.structure.SqlG;
import org.umlg.sqlg.structure.SqlgVertex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Date: 2014/08/16
 * Time: 6:49 PM
 */
public class ResultSetIterator implements Iterator<Vertex> {

    private ResultSet resultSet;
    private SqlG sqlG;

    public ResultSetIterator(SqlG sqlG, ResultSet resultSet) {
        this.sqlG = sqlG;
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            return !resultSet.isLast();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Vertex next() {
        try {
            resultSet.next();
            long id = resultSet.getLong(1);
            String schema = resultSet.getString(2);
            String table = resultSet.getString(3);
            SqlgVertex sqlGVertex = SqlgVertex.of(this.sqlG, id, schema, table);
            return sqlGVertex;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
