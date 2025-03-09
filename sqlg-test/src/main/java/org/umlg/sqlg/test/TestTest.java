package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "ab");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "ac");

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "bc");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "bd");

        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "ca");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "cb");

        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);

        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").as("a")
                .out("ab").as("b")
                .optional(
                        __.out("bc").as("c")
                        .order()
                        .by(__.select("c").by("name"), Order.desc)
                )
                .order()
                .by(__.select("a").by("name"), Order.asc)
                .by(__.select("b").by("name"), Order.asc)
                .path().toList();
        for (Path path : paths) {
            LOGGER.info(path.toString());
        }

    }

}
