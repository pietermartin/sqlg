package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2016/11/08
 * Time: 5:09 AM
 */
public class TestTopologyMultipleGraphs extends BaseTest {

    @SuppressWarnings("Duplicates")
    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeepEqualsPublicSchema() {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            a1.property("test", "asdasd");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            a1.addEdge("ab", b1);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb1");
            a1.addEdge("ab", bb1);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            Vertex aa1 = this.sqlgGraph.addVertex(T.label, "AA", "name", "aa1");
            Edge ab = aa1.addEdge("ab", bb1);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            ab.property("test", "asdasd");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeepEqualsAcrossSchema() {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            a1.property("test", "asdasd");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "name", "b1");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            a1.addEdge("ab", b1);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB.BB", "name", "bb1");
            a1.addEdge("ab", bb1);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            Vertex aa1 = this.sqlgGraph.addVertex(T.label, "AA.AA", "name", "aa1");
            Edge ab = aa1.addEdge("ab", bb1);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            ab.property("test", "asdasd");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());

            assertEquals(4, this.sqlgGraph.traversal().V().count().next(), 0);
            assertEquals(4, sqlgGraph1.traversal().V().count().next(), 0);
            assertEquals(3, this.sqlgGraph.traversal().E().count().next(), 0);
            assertEquals(3, sqlgGraph1.traversal().E().count().next(), 0);

            assertEquals(2, this.sqlgGraph.traversal().V(a1.id()).out("ab").count().next(), 0);
            assertEquals(2, sqlgGraph1.traversal().V(a1.id()).out("ab").count().next(), 0);
            assertEquals(1, this.sqlgGraph.traversal().V(aa1.id()).out("ab").count().next(), 0);
            assertEquals(1, sqlgGraph1.traversal().V(aa1.id()).out("ab").count().next(), 0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testModernAcrossGraphs() {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            loadModern();
            Thread.sleep(1000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGratefulDeadAcrossGraphs() {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            loadGratefulDead();
            Thread.sleep(1000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
