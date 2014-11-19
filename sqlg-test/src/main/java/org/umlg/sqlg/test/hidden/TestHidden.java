package org.umlg.sqlg.test.hidden;

import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.TriFunction;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2014/11/19
 * Time: 10:35 AM
 */
public class TestHidden extends BaseTest {

    Iterable<Object[]> result;

    @Before
    public void before() throws IOException {
        super.before();
        final List<Pair<String, TriFunction<Graph, Vertex, Boolean, Boolean>>> tests = new ArrayList<>();
        tests.add(Pair.with("v.property(\"age\").isPresent()", (Graph g, Vertex v, Boolean multi) -> v.property("age").isPresent()));
        tests.add(Pair.with("v.value(\"age\").equals(16)", (Graph g, Vertex v, Boolean multi) -> v.value("age").equals(16)));
        tests.add(Pair.with("v.properties(\"age\").count().next().intValue() == 1", (Graph g, Vertex v, Boolean multi) -> v.properties("age").count().next().intValue() == 1));
        tests.add(Pair.with("v.properties(\"age\").value().next().equals(16)", (Graph g, Vertex v, Boolean multi) -> v.properties("age").value().next().equals(16)));
        tests.add(Pair.with("v.hiddens(\"age\").count().next().intValue() == 0", (Graph g, Vertex v, Boolean multi) -> v.hiddens("age").count().next().intValue() == 0));
        tests.add(Pair.with("v.hiddens(Graph.Key.hide(\"age\")).count().next().intValue() == 2", (Graph g, Vertex v, Boolean multi) -> v.hiddens(Graph.Key.hide("age")).count().next().intValue() == (multi ? 2 : 1)));
        tests.add(Pair.with("v.properties(Graph.Key.hide(\"age\")).count().next() == 0", (Graph g, Vertex v, Boolean multi) -> v.properties(Graph.Key.hide("age")).count().next().intValue() == 0));
        tests.add(Pair.with("v.propertyMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Vertex v, Boolean multi) -> v.propertyMap(Graph.Key.hide("age")).next().size() == 0));
        tests.add(Pair.with("v.valueMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Vertex v, Boolean multi) -> v.valueMap(Graph.Key.hide("age")).next().size() == 0));
        tests.add(Pair.with("v.propertyMap(\"age\").next().size() == 1", (Graph g, Vertex v, Boolean multi) -> v.propertyMap("age").next().size() == 1));
        tests.add(Pair.with("v.valueMap(\"age\").next().size() == 1", (Graph g, Vertex v, Boolean multi) -> v.valueMap("age").next().size() == 1));
        tests.add(Pair.with("v.hiddenMap(Graph.Key.hide(\"age\")).next().size() == 1", (Graph g, Vertex v, Boolean multi) -> v.hiddenMap(Graph.Key.hide("age")).next().size() == 1));
        tests.add(Pair.with("v.hiddenMap(\"age\").next().size() == 0", (Graph g, Vertex v, Boolean multi) -> v.hiddenMap("age").next().size() == 0));
        tests.add(Pair.with("v.hiddenValueMap(Graph.Key.hide(\"age\")).next().size() == 1", (Graph g, Vertex v, Boolean multi) -> v.hiddenValueMap(Graph.Key.hide("age")).next().size() == 1));
        tests.add(Pair.with("v.hiddenValueMap(\"age\").next().size() == 0", (Graph g, Vertex v, Boolean multi) -> v.hiddenValueMap("age").next().size() == 0));
        tests.add(Pair.with("v.hiddens(Graph.Key.hide(\"age\")).value().toList().contains(34)", (Graph g, Vertex v, Boolean multi) -> multi ? v.hiddens(Graph.Key.hide("age")).value().toList().contains(34) : v.hiddens(Graph.Key.hide("age")).value().toList().contains(29)));
        tests.add(Pair.with("v.hiddens(Graph.Key.hide(\"age\")).value().toList().contains(29)", (Graph g, Vertex v, Boolean multi) -> v.hiddens(Graph.Key.hide("age")).value().toList().contains(29)));
        tests.add(Pair.with("v.hiddenKeys().size() == 2", (Graph g, Vertex v, Boolean multi) -> v.hiddenKeys().size() == 2));
        tests.add(Pair.with("v.keys().size() == 3", (Graph g, Vertex v, Boolean multi) -> v.keys().size() == 3));
        tests.add(Pair.with("v.keys().contains(\"age\")", (Graph g, Vertex v, Boolean multi) -> v.keys().contains("age")));
        tests.add(Pair.with("v.keys().contains(\"name\")", (Graph g, Vertex v, Boolean multi) -> v.keys().contains("name")));
        tests.add(Pair.with("v.hiddenKeys().contains(Graph.Key.hide(\"age\"))", (Graph g, Vertex v, Boolean multi) -> v.hiddenKeys().contains(Graph.Key.hide("age"))));
        tests.add(Pair.with("v.property(Graph.Key.hide(\"color\")).key().equals(Graph.Key.hide(\"color\"))", (Graph g, Vertex v, Boolean multi) -> v.property(Graph.Key.hide("color")).key().equals(Graph.Key.hide("color"))));
        tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide(\"color\"))).count() == 1", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide("color"))).count() == 1));
        tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide(\"age\"))).count() == 2", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide("age"))).count() == (multi ? 2 : 1)));
        tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(\"age\")).count() == 1", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().propertyIterator("age")).count() == 1));

        result = tests.stream().map(d -> {
            final Object[] o = new Object[2];
            o[0] = d.getValue0();
            o[1] = d.getValue1();
            return o;
        }).collect(Collectors.toList());

    }

    @Test
    public void shouldHandleHiddenVertexProperties() {
        try {
            final Vertex v = this.sqlgGraph.addVertex(Graph.Key.hide("age"), 29, "age", 16, "name", "marko", "food", "taco", Graph.Key.hide("color"), "purple");
            this.sqlgGraph.tx().commit();
            boolean i = v.hiddenKeys().contains(Graph.Key.hide("age"));
            for (Object[] o : result) {
                TriFunction<Graph, Vertex, Boolean, Boolean> streamGetter = (TriFunction<Graph, Vertex, Boolean, Boolean>) o[1];
                Assert.assertTrue((String)o[0], streamGetter.apply(this.sqlgGraph, v, false));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldReturnHiddenKeysWithHiddenPrefix() {
        final Vertex v = this.sqlgGraph.addVertex("name", "marko", Graph.Key.hide("acl"), "rw", Graph.Key.hide("other"), "rw", "acl", "r");
        this.sqlgGraph.tx().commit();
        final Vertex v1 = this.sqlgGraph.v(v.id());
        assertEquals(2, v1.hiddenKeys().size());
        assertTrue(v1.hiddenKeys().stream().allMatch(key -> Graph.Key.isHidden(key)));
        assertTrue(v1.hiddenKeys().stream().allMatch(k -> k.equals(Graph.Key.hide("acl")) || k.equals(Graph.Key.hide("other"))));
        //   assertEquals("rw", v1.iterators().hiddenPropertyIterator(Graph.Key.hide("acl")).next().value());
        assertEquals("r", v1.iterators().propertyIterator("acl").next().value());
        assertEquals(Graph.Key.hide("acl"), v1.property(Graph.Key.hide("acl")).key());
        assertEquals(Graph.Key.hide("other"), v1.property(Graph.Key.hide("other")).key());
    }

}
