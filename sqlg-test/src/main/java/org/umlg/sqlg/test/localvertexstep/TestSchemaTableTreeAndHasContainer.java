package org.umlg.sqlg.test.localvertexstep;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2016/08/27
 * Time: 6:58 PM
 */
public class TestSchemaTableTreeAndHasContainer  extends BaseTest {

    /**
     *  This example is a slightly modified version of GitHub issue 63.
     *  What happens here is that all vertex labels have a contains edge.
     *  At present Sqlg will try to join to all other foreign keys on the contains edge.
     *  Even to vertex labels that it is not really attached to. Dum but there you have it, its a TODO now that I know.
     *  However those vertex labels do not have '__structuredDataKey' so those nodes should be removed from the SchemaTableTree.
     *  This happened but as the leafNodes were calculated before the nodes were removed they stayed behind in the leafNode cache.
     *  Now the leafNode cache is calculated afterwards and all is well again.
     */
    @Test
    public void issue63() {
        Vertex tnt = sqlgGraph.addVertex(T.label, "tenant", "__type", "tenant");
        Vertex env = sqlgGraph.addVertex(T.label, "environment", "__type", "environment");
        Vertex res = sqlgGraph.addVertex(T.label, "resource", "__type", "resource");
        Vertex de = sqlgGraph.addVertex(T.label, "dataEntity", "__type", "dataEntity");
        Vertex dRoot = sqlgGraph.addVertex(T.label, "structuredData", "__type", "structuredData");
        Vertex dPrims = sqlgGraph.addVertex(T.label, "structuredData", "__type", "structuredData", "__structuredDataKey", "primitives");
        Vertex d0 = sqlgGraph.addVertex(T.label, "structuredData", "__type", "structuredData", "__structuredDataIndex", 0);

        tnt.addEdge("contains", env);
        env.addEdge("contains", res);
        res.addEdge("contains", de);
        de.addEdge("hasData", dRoot);
        dRoot.addEdge("contains", dPrims);
        dPrims.addEdge("contains", d0);

        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> results = gt
                .V(res)
                .out("contains").has("__type", "dataEntity")
                .local(
                        __.out("hasData")
                                .out("contains").has("__type", "structuredData").has("__structuredDataKey", "primitives")
                                .out("contains").has("__type", "structuredData")
                );
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(d0, results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testHasContainerClearsNodeFromSchemaTableTree() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "nameA", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "nameB", "b");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "nameC", "c");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D");
        Vertex e1 = this.sqlgGraph.addVertex(T.label, "E");
        a1.addEdge("e", b1);
        a1.addEdge("e", d1);
        b1.addEdge("e", c1);
        d1.addEdge("e", e1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1)
                .local(
                        __.out("e").has("nameB", "b")
                                .out("e")
                ).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(c1, vertices.get(0));
    }
}
