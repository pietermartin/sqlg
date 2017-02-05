package org.umlg.sqlg.test.batch;

import org.junit.*;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;

/**
 * Date: 2015/12/31
 * Time: 9:14 AM
 */
public class TestBatchTemporaryVertex extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (configuration.getString("jdbc.url").contains("postgresql")) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testTempBatch() throws SQLException {

        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 1000; i++) {
            this.sqlgGraph.streamTemporaryVertex("halo", new LinkedHashMap<String, Object>(){{put("this", "that");}});
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement s = conn.prepareStatement("select * from \"V_halo\"")) {
            Assert.assertEquals("", s.getMetaData().getSchemaName(1));
            ResultSet resultSet = s.executeQuery();
            while (resultSet.next()) {
                count++;
                Assert.assertEquals("that", resultSet.getString(2));
            }
        }
        Assert.assertEquals(1000, count);
        this.sqlgGraph.tx().commit();

    }
}
