package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;

import java.net.URL;

/**
 * Date: 2016/02/06
 * Time: 6:17 PM
 */
public class TestSqlgSchemaIncorrectIndexNames {

    protected static Configuration configuration;
    protected SqlgGraph sqlgGraph;

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() throws Exception {
        this.sqlgGraph = SqlgGraph.open(configuration);
//        SqlgUtil.dropDb(this.sqlgGraph);
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.close();
//        this.sqlgGraph = SqlgGraph.open(configuration);
//        assertNotNull(this.sqlgGraph);
//        assertNotNull(this.sqlgGraph.getBuildVersion());
    }

    @Test
    public void testTopologyUpgradeForeignKey() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            System.out.println("asd");
        }
    }

}
