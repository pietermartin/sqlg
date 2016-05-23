package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/12/14
 * Time: 8:56 PM
 */
public class TestNormalBatchUpdate extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testUpdateWithoutDots() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 4; i++) {
            this.sqlgGraph.addVertex(T.label, "GTRX", "test1", "a1", "test2", "a2", "cm_uid", "cm_uid_" + i);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        GraphTraversalSource gts = this.sqlgGraph.traversal();
        Vertex cmUid0 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next();
        cmUid0.property("test1", "b1");

        Vertex cmUid1 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next();
        cmUid1.property("test2", "b1");

        Vertex cmUid2 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next();
        cmUid2.property("test1", "b1");

        Vertex cmUid3 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next();
        cmUid3.property("test2", "b1");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test1"));
        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test2"));

        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test1"));
        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test2"));

        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test1"));
        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test2"));

        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test1"));
        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test2"));
    }

    @Test
    public void testUpdateWithDots() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 4; i++) {
            this.sqlgGraph.addVertex(T.label, "GTRX", "test1", "a1", "test.2", "a2", "cm_uid", "cm_uid_" + i);
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        GraphTraversalSource gts = this.sqlgGraph.traversal();
        Vertex cmUid0 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next();
        cmUid0.property("test1", "b1");

        Vertex cmUid1 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next();
        cmUid1.property("test.2", "b1");

        Vertex cmUid2 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next();
        cmUid2.property("test1", "b1");

        Vertex cmUid3 = gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next();
        cmUid3.property("test.2", "b1");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test1"));
        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_0").next().value("test.2"));

        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test1"));
        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_1").next().value("test.2"));

        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test1"));
        Assert.assertEquals("a2", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_2").next().value("test.2"));

        Assert.assertEquals("a1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test1"));
        Assert.assertEquals("b1", gts.V().hasLabel("GTRX").has("cm_uid", "cm_uid_3").next().value("test.2"));
    }

    @Test
    public void testUpdateArrays() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person",
                "names", new String[]{"A", "B"},
                "integers", new int[]{1, 2},
                "booleans", new boolean[]{true, true},
                "doubles", new double[]{1D, 2D},
                "longs", new long[]{1L, 2L},
                "floats", new float[]{1F, 2F},
                "shorts", new short[]{1, 2},
                "bytes", new byte[]{1, 2}
        );
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").next().property("names").isPresent());
        Assert.assertArrayEquals(new String[]{"A", "B"}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<String[]>value("names"));
        Assert.assertArrayEquals(new byte[]{1, 2}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
        this.sqlgGraph.tx().normalBatchModeOn();
        v.property("names", new String[]{"C", "D"});
        v.property("integers", new int[]{3, 4});
        v.property("booleans", new boolean[]{false, false});
        v.property("doubles", new double[]{3D, 4D});
        v.property("longs", new long[]{3L, 4L});
        v.property("floats", new float[]{3F, 4F});
        v.property("shorts", new short[]{3, 4});
        v.property("bytes", new byte[]{3, 4});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new String[]{"C", "D"}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<String[]>value("names"));
        Assert.assertArrayEquals(new int[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<int[]>value("integers"));
        Assert.assertArrayEquals(new boolean[]{false, false}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<boolean[]>value("booleans"));
        Assert.assertArrayEquals(new double[]{3D, 4D}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<double[]>value("doubles"), 0d);
        Assert.assertArrayEquals(new long[]{3L, 4L}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<long[]>value("longs"));
        Assert.assertArrayEquals(new float[]{3F, 4F}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<float[]>value("floats"), 0f);
        Assert.assertArrayEquals(new short[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<short[]>value("shorts"));
        Assert.assertArrayEquals(new byte[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
    }

    @Test
    public void testStringUpdate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", "halo");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("name", "halo3");
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals("halo3", v1.value("name"));
    }

    @Test
    public void testStringUpdateEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e1 = v1.addEdge("test", v2, "name", "halo");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        e1.property("name", "halo3");
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Assert.assertEquals("halo3", e1.value("name"));
    }

    @Test
    public void testBooleanUpdate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Boolean.valueOf(true));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("name", Boolean.valueOf(false));
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals(Boolean.valueOf(false), v1.value("name"));
    }

    @Test
    public void testBooleanUpdateEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e1 = v1.addEdge("test", v2, "name", Boolean.valueOf(true));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        e1.property("name", Boolean.valueOf(false));
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Assert.assertEquals(Boolean.valueOf(false), e1.value("name"));
        Assert.assertTrue(e1.value("name").getClass().equals(Boolean.class));
    }

    @Test
    public void testShortUpdate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Short.valueOf("1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("name", Short.valueOf("2"));
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals(Short.valueOf("2"), v1.value("name"));
    }

    @Test
    public void testShortUpdateEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e1 = v1.addEdge("test", v2, "name", Short.valueOf("1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        e1.property("name", Short.valueOf("2"));
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Assert.assertEquals(Short.valueOf("2"), e1.value("name"));
    }

    @Test
    public void testIntegerUpdate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Integer.valueOf("1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("name", Integer.valueOf("2"));
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals(Integer.valueOf("2"), v1.value("name"));
    }

    @Test
    public void testIntegerUpdateEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e1 = v1.addEdge("test", v2, "name", Integer.valueOf("1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        e1.property("name", Integer.valueOf("2"));
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Assert.assertEquals(Integer.valueOf("2"), e1.value("name"));
    }

    @Test
    public void testLongUpdate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Long.valueOf("1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("name", Long.valueOf("2"));
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals(Long.valueOf("2"), v1.value("name"));
    }

    @Test
    public void testLongUpdateEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e1 = v1.addEdge("test", v2, "name", Long.valueOf("1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        e1.property("name", Long.valueOf("2"));
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Assert.assertEquals(Long.valueOf("2"), e1.value("name"));
    }

    @Test
    public void testFloatUpdate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Float.valueOf("1.1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("name", Float.valueOf("2.2"));
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals(Float.valueOf("2.2"), v1.value("name"));
    }

    @Test
    public void testFloatUpdateEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e1 = v1.addEdge("test", v2, "name", Float.valueOf("1.1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        e1.property("name", Float.valueOf("2.2"));
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Assert.assertEquals(Float.valueOf("2.2"), e1.value("name"));
    }

    @Test
    public void testDoubleUpdate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", Double.valueOf("1.1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("name", Double.valueOf("2.2"));
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals(Double.valueOf("2.2"), v1.value("name"));
    }

    @Test
    public void testDoubleUpdateEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "A");
        Edge e1 = v1.addEdge("test", v2, "name", Double.valueOf("1.1"));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        e1.property("name", Double.valueOf("2.2"));
        this.sqlgGraph.tx().commit();
        e1 = this.sqlgGraph.traversal().E(e1.id()).next();
        Assert.assertEquals(Double.valueOf("2.2"), e1.value("name"));
    }

}
