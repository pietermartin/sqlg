package org.umlg.sqlg.test.vertexout;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.matchers.StartsWith;
import org.umlg.sqlg.process.graph.util.SqlgVertexStep;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2014/07/13
 * Time: 4:48 PM
 */
public class TestVertexOutWithHas extends BaseTest {

    @Test
    public void testVertexOutWithHas() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex bmw1 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw", "cc", 600);
        Vertex bmw2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw", "cc", 800);
        Vertex ktm1 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 200);
        Vertex ktm2 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 200);
        Vertex ktm3 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 400);
        marko.addEdge("drives", bmw1);
        marko.addEdge("drives", bmw2);
        marko.addEdge("drives", ktm1);
        marko.addEdge("drives", ktm2);
        marko.addEdge("drives", ktm3);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = marko.out("drives").<Vertex>has("name", "bmw");
        traversal.asAdmin().applyStrategies(TraversalEngine.STANDARD);
        final List<Step> temp = new ArrayList<>();
        Step currentStep = TraversalHelper.getStart(traversal.asAdmin());
        while (!(currentStep instanceof EmptyStep)) {
            temp.add(currentStep);
            currentStep = currentStep.getNextStep();
        }
        Assert.assertTrue(temp.get(0) instanceof StartStep);
        Assert.assertTrue(temp.get(1) instanceof SqlgVertexStep);
        Assert.assertEquals(2, temp.size());
        List<Vertex> drivesBmw = marko.out("drives").<Vertex>has("name", "bmw").toList();
        Assert.assertEquals(2L, drivesBmw.size(), 0);
        List<Vertex> drivesKtm = marko.out("drives").<Vertex>has("name", "ktm").toList();
        Assert.assertEquals(3L, drivesKtm.size(), 0);

        List<Vertex> cc600 = marko.out("drives").<Vertex>has("cc", 600).toList();
        Assert.assertEquals(1L, cc600.size(), 0);
        List<Vertex> cc800 = marko.out("drives").<Vertex>has("cc", 800).toList();
        Assert.assertEquals(1L, cc800.size(), 0);
        List<Vertex> cc200 = marko.out("drives").<Vertex>has("cc", 200).toList();
        Assert.assertEquals(2L, cc200.size(), 0);
        List<Vertex> cc400 = marko.out("drives").<Vertex>has("cc", 400).toList();
        Assert.assertEquals(1L, cc400.size(), 0);
    }

    @Test
    public void testVertexInWithHas() {
        Vertex marko1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko1");
        Vertex marko2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko2", "age", 20);
        Vertex marko3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko3", "age", 20);
        Vertex marko4 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko4", "age", 30);
        Vertex marko5 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko5", "age", 40);

        marko2.addEdge("friend", marko1);
        marko3.addEdge("friend", marko1);
        marko4.addEdge("friend", marko1);
        marko5.addEdge("friend", marko1);

        this.sqlgGraph.tx().commit();

        List<Vertex> inFriendMarko2 = marko1.in("friend").<Vertex>has("name", "marko2").toList();
        Assert.assertEquals(1, inFriendMarko2.size(), 0);
        List<Vertex> inFriendMarko3 = marko1.in("friend").<Vertex>has("name", "marko3").toList();
        Assert.assertEquals(1, inFriendMarko3.size(), 0);
        List<Vertex> inFriendMarko4 = marko1.in("friend").<Vertex>has("name", "marko4").toList();
        Assert.assertEquals(1, inFriendMarko4.size(), 0);
        List<Vertex> inFriendMarko5 = marko1.in("friend").<Vertex>has("name", "marko5").toList();
        Assert.assertEquals(1, inFriendMarko5.size(), 0);

        List<Vertex> inFriendAge20 = marko1.in("friend").<Vertex>has("age", 20).toList();
        Assert.assertEquals(2, inFriendAge20.size(), 0);
        List<Vertex> inFriendAge30 = marko1.in("friend").<Vertex>has("age", 30).toList();
        Assert.assertEquals(1, inFriendAge30.size(), 0);
        List<Vertex> inFriendAge40 = marko1.in("friend").<Vertex>has("age", 40).toList();
        Assert.assertEquals(1, inFriendAge40.size(), 0);

        List<Vertex> inFriendNameMarko3Age20 = marko1.in("friend").<Vertex>has("age", 20).<Vertex>has("name", "marko3").toList();
        Assert.assertEquals(1, inFriendNameMarko3Age20.size(), 0);

        List<Vertex> inFriendNameMarko4Age20 = marko1.in("friend").<Vertex>has("age", 20).<Vertex>has("name", "marko4").toList();
        Assert.assertEquals(0, inFriendNameMarko4Age20.size(), 0);

    }

    @Test
    public void testVertexHasLabel() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex bmw = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        Vertex vw = this.sqlgGraph.addVertex(T.label, "Car", "name", "vw");
        marko.addEdge("drives", bmw);
        marko.addEdge("drives", vw);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, marko.out("drives").has(T.label, "Car").has("name", "bmw").count().next(), 0);
        Assert.assertEquals(0, marko.out("drives").has(T.label, "Person").has("name", "vw").count().next(), 0);


    }

}
