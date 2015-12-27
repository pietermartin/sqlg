package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/12/14
 * Time: 8:56 PM
 */
public class TestBatchUpdate extends BaseTest {

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
        int count = 1;
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
        int count = 1;
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
                "doubles", new double[]{1d, 2d},
                "longs", new long[]{1l, 2l},
                "floats", new float[]{1f, 2f},
                "shorts", new short[]{1, 2},
                "bytes", new byte[]{1, 2}
        );
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").hasNext());
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("Person").next().property("names").isPresent());
        Assert.assertArrayEquals(new String[]{"A", "B"}, this.sqlgGraph.traversal().V().hasLabel("Person").next().value("names"));
        Assert.assertArrayEquals(new byte[]{1, 2}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
        this.sqlgGraph.tx().normalBatchModeOn();
        v.property("names", new String[]{"C", "D"});
        v.property("integers", new int[]{3, 4});
        v.property("booleans", new boolean[]{false, false});
        v.property("doubles", new double[]{3d, 4d});
        v.property("longs", new long[]{3l, 4l});
        v.property("floats", new float[]{3f, 4f});
        v.property("shorts", new short[]{3, 4});
        v.property("bytes", new byte[]{3, 4});
        this.sqlgGraph.tx().commit();
        Assert.assertArrayEquals(new String[]{"C", "D"}, this.sqlgGraph.traversal().V().hasLabel("Person").next().value("names"));
        Assert.assertArrayEquals(new int[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().value("integers"));
        Assert.assertArrayEquals(new boolean[]{false, false}, this.sqlgGraph.traversal().V().hasLabel("Person").next().value("booleans"));
        Assert.assertArrayEquals(new double[]{3d, 4d}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<double[]>value("doubles"), 0d);
        Assert.assertArrayEquals(new long[]{3l, 4l}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<long[]>value("longs"));
        Assert.assertArrayEquals(new float[]{3f, 4f}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<float[]>value("floats"), 0f);
        Assert.assertArrayEquals(new short[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<short[]>value("shorts"));
        Assert.assertArrayEquals(new byte[]{3, 4}, this.sqlgGraph.traversal().V().hasLabel("Person").next().<byte[]>value("bytes"));
    }
}
