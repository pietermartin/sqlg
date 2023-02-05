package org.umlg.sqlg.test.datasource;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.sql.*;
import java.util.HashMap;

public class TestStalePreparedStatement extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            String jdbcbUrl = configuration.getString("jdbc.url");
            //disable prepared statement caching and set the autosave to never. This will override the PostgresPlugin
            // setting autosave=conservative.
            configuration.setProperty("jdbc.url", jdbcbUrl + "?prepareThreshold=1&autosave=never");
        }
    }

    /**
     * This is to test the behavior around, <a href="https://github.com/pgjdbc/pgjdbc/pull/451">pgjdbc/a>
     * Setting autosave=never, i.e. the default will allow the exception to occur.
     */
    @Test
    public void testStatePreparedStateCacheExceptionScenario() {
        Assume.assumeTrue(isPostgres());
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new HashMap<>() {{
                    put("a", PropertyDefinition.of(PropertyType.STRING));
                }}
        );
        this.sqlgGraph.addVertex(T.label, "Person", "a", "a1");
        this.sqlgGraph.tx().commit();

        int prepareThreshold = 2;
        for (int i = 0; i < prepareThreshold; i++) {
            Connection connection = this.sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = connection.prepareStatement("select * from \"V_Person\"")) {
                ResultSet rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            this.sqlgGraph.tx().commit();
        }
        //only now will it do a server side prepared statement
        vertexLabel.ensurePropertiesExist(new HashMap<>() {{
            put("b", PropertyDefinition.of(PropertyType.STRING));
        }});
        this.sqlgGraph.tx().commit();
        Connection connection = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement("select * from \"V_Person\"")) {
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            Assert.fail("Expected PSQLException, 'ERROR: cached plan must not change result type'");
        } catch (Exception e) {
            Assert.assertEquals("org.postgresql.util.PSQLException", e.getClass().getName());
            Assert.assertEquals("ERROR: cached plan must not change result type", e.getMessage());
        }
    }

}
