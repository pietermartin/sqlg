package org.umlg.sqlg.test.vertex;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/07/26
 */
public class TestAddTemporaryVertex extends BaseTest {

    @Test
    public void testAddTemporaryVertex() throws SQLException {
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addTemporaryVertex(T.label, "A", "name", "halo");
        }
        int count = 0;
        Connection conn = this.sqlgGraph.tx().getConnection();
        String sql;
        if (this.sqlgGraph.getSqlDialect().needsTemporaryTableSchema()) {
            sql = "select * from " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) +
                    "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A");
        } else {
            if (!this.sqlgGraph.getSqlDialect().needsTemporaryTablePrefix()) {
                sql = "select * from " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A");
            } else {
                sql = "select * from " + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().temporaryTablePrefix() + "V_A");
            }
        }
        try (PreparedStatement s = conn.prepareStatement(sql)) {
            ResultSet resultSet = s.executeQuery();
            while (resultSet.next()) {
                count++;
                Assert.assertEquals("halo", resultSet.getString(2));
            }
        }
        Assert.assertEquals(10, count);
        this.sqlgGraph.tx().commit();
    }

}
