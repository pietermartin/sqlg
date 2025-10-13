package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class TestTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTest.class);

//    @BeforeClass
//    public static void beforeClass() {
//        PropertiesConfiguration configuration = new PropertiesConfiguration();
//        configuration.addProperty("jdbc.url", "jdbc:postgresql://localhost:5432/cm-dist");

    /// /        configuration.addProperty("jdbc.url", "jdbc:postgresql://10.70.41.151:5432/cm");
//        configuration.addProperty("jdbc.username", "cm");
//        configuration.addProperty("jdbc.password", "cm");
//        SqlgGraph sqlgGraph = SqlgGraph.open(configuration);
//        sqlgGraph.close();
//    }
//
//    @Before
//    public void before() throws Exception {
//
//    }

    @Test
    public void test() {

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "d", "d", "a", "a", "b", "b", "c", "c");
        this.sqlgGraph.tx().commit();
        List<Map<Object, Object>> list =  this.sqlgGraph.traversal().V().hasLabel("A").elementMap().toList();
        for (Map<Object, Object> map: list) {
            LOGGER.info(map.toString());
        }
        List<Object> values = sqlgGraph.traversal().V().hasLabel("A").values().toList();
        for (Object value : values) {
            LOGGER.info(value.toString());
        }

    }

}
