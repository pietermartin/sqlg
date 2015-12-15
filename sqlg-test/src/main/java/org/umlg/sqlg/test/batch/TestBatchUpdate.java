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
}
