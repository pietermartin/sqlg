package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/07/27
 */
public class TestBatchTemporaryVertex extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestBatchTemporaryVertex.class);

    @Test
    public void testBatchTempVertex() throws SQLException {
        int INSERT_COUNT = 1_000_000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < INSERT_COUNT; i++) {
            this.sqlgGraph.addTemporaryVertex(T.label, "A", "name", "halo");
            if (i % 10_000 == 0) {
                this.sqlgGraph.tx().flush();
            }
        }
        this.sqlgGraph.tx().flush();
        stopWatch.stop();
        LOGGER.info(stopWatch.toString());
        int count = 0;
        Connection conn = this.sqlgGraph.tx().getConnection();
        String sql = "select * from ";
        if (this.sqlgGraph.getSqlDialect().needsTemporaryTableSchema()) {
            sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) +
                    ".";
        }
        if (!this.sqlgGraph.getSqlDialect().needsTemporaryTablePrefix()) {
            sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("V_A");
        } else {
            sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                    this.sqlgGraph.getSqlDialect().temporaryTablePrefix() +
                    "V_A");
        }
        try (PreparedStatement s = conn.prepareStatement(sql)) {
            ResultSet resultSet = s.executeQuery();
            while (resultSet.next()) {
                count++;
                Assert.assertEquals("halo", resultSet.getString(2));
            }
        }
        Assert.assertEquals(INSERT_COUNT, count);
    }

}
