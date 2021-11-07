package org.umlg.sqlg.test.union;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Date: 2016/05/30
 * Time: 9:01 PM
 */
@SuppressWarnings("unchecked")
public class TestUnion extends BaseTest {

    @Test
    public void g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
        loadModern();
        final Traversal<Vertex, Map<String, Long>> traversal =  (Traversal) this.sqlgGraph.traversal().V().union(
                        repeat(union(
                                out("created"),
                                in("created"))).times(2),
                        repeat(union(
                                in("created"),
                                out("created"))).times(2))
                .label().groupCount();
        final Map<String, Long> groupCount = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, groupCount.size());
        assertEquals(12L, groupCount.get("software").longValue());
        assertEquals(20L, groupCount.get("person").longValue());
    }

    /**
     * https://github.com/pietermartin/sqlg/issues/416
     */
    @Test
    public void testAliasesWithinUnion() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "B");

        a.addEdge("edge", b);

        // First, confirm that a simple traversal using an alias works properly.  This works as
        // expected (the failure is below with the union where the exact traversal is used inside the union).
        List<Vertex> noUnionItems = this.sqlgGraph.traversal()
                .V()
                .hasLabel("A")
                .as("alias1")
                .out()
                .<Vertex>select("alias1")
                .toList();

        Assert.assertEquals(1, noUnionItems.size());
        Assert.assertEquals(a, noUnionItems.get(0));

        // This one doesn't work even though the exact same traversal is used inside the union.  Debugging
        // the code shows that it cannot find the "alias1" label in the SelectOneStep (in the map method).
        List<Vertex> unionItems = this.sqlgGraph.traversal()
                .inject("ignore")       // Normally an inject would be used here, but see #415
                .<Vertex>union(
                        __.V().hasLabel("A").as("alias1").out().select("alias1")
                )
                .toList();

        // This fails because unionItems contains 0 results.
        Assert.assertEquals(1, unionItems.size());
    }

    @Test
    public void testdkarthikeyan88_bug359() {
        Graph g = this.sqlgGraph;
        Vertex cluster = g.addVertex(T.label, "Cluster", "name", "Test Cluster");
        Vertex service = g.addVertex(T.label, "Service", "name", "Test Service");
        Vertex database = g.addVertex(T.label, "Database", "name", "Test DB");
        Vertex schema1 = g.addVertex(T.label, "Schema", "name", "Test Schema1");
        Vertex schema2 = g.addVertex(T.label, "Schema", "name", "Test Schema2");
        Vertex table1 = g.addVertex(T.label, "Table", "name", "Table1");
        Vertex table2 = g.addVertex(T.label, "Table", "name", "Table2");
        Vertex table3 = g.addVertex(T.label, "Table", "name", "Table3");
        Vertex table4 = g.addVertex(T.label, "Table", "name", "Table4");
        Vertex column1 = g.addVertex(T.label, "Column", "name", "Column1");
        Vertex column2 = g.addVertex(T.label, "Column", "name", "Column2");
        Vertex column3 = g.addVertex(T.label, "Column", "name", "Column3");
        Vertex column4 = g.addVertex(T.label, "Column", "name", "Column4");
        Vertex column5 = g.addVertex(T.label, "Column", "name", "Column5");
        Vertex column6 = g.addVertex(T.label, "Column", "name", "Column6");
        Vertex column7 = g.addVertex(T.label, "Column", "name", "Column7");
        Vertex column8 = g.addVertex(T.label, "Column", "name", "Column8");

        cluster.addEdge("has_Service", service);
        service.addEdge("has_Database", database);
        database.addEdge("has_Schema", schema1);
        database.addEdge("has_Schema", schema2);
        schema1.addEdge("has_Table", table1);
        schema1.addEdge("has_Table", table2);
        schema2.addEdge("has_Table", table3);
        schema2.addEdge("has_Table", table4);
        table1.addEdge("has_Column", column1);
        table1.addEdge("has_Column", column2);
        table2.addEdge("has_Column", column3);
        table2.addEdge("has_Column", column4);
        table3.addEdge("has_Column", column5);
        table3.addEdge("has_Column", column6);
        table4.addEdge("has_Column", column7);
        table4.addEdge("has_Column", column8);

        g.tx().commit();

//        String expected = "" +
//                "{" +
//                "   Test Cluster={" +
//                "       Test Service={" +
//                "           Test DB={" +
//                "               Test Schema1={" +
//                "                   Table1={" +
//                "                       Column1={}, Column2={}" +
//                "                   }" +
//                "               }, " +
//                "               Test Schema2={" +
//                "                   Table3={" +
//                "                       Column5={}, Column6={}" +
//                "                   }" +
//                "               }" +
//                "           }" +
//                "       }" +
//                "   }" +
//                "}";

        GraphTraversal<Vertex, Tree> traversal = g.traversal().V()
                .hasLabel("public.Cluster")
                .has("name", "Test Cluster")
                .out("has_Service").has("name", "Test Service")
                .out("has_Database").has("name", "Test DB")
                .union(
                        __.out("has_Schema").has("name", P.eq("Test Schema1")).out("has_Table").has("name", P.without("Table2")),
                        __.out("has_Schema").has("name", P.eq("Test Schema1")).out("has_Table").has("name", P.within("Table1")),
                        __.out("has_Schema").has("name", P.eq("Test Schema2")).out("has_Table").has("name", P.neq("Table4")))
                .out("has_Column")
                .range(0, 100).tree();

        Tree<Vertex> tree = traversal.next();

        List<Vertex> clusters = tree.getObjectsAtDepth(1);
        Assert.assertEquals(1, clusters.size());
        List<Vertex> services = tree.getObjectsAtDepth(2);
        Assert.assertEquals(1, services.size());
        List<Vertex> databases = tree.getObjectsAtDepth(3);
        Assert.assertEquals(1, databases.size());
        List<Vertex> schemas = tree.getObjectsAtDepth(4);
        Assert.assertEquals(2, schemas.size());
        List<Vertex> tables = tree.getObjectsAtDepth(5);
        Assert.assertEquals(2, tables.size());
        List<Vertex> columns = tree.getObjectsAtDepth(6);
        Assert.assertEquals(4, columns.size());
        columns = tree.getLeafObjects();
        Assert.assertEquals(4, columns.size());

        Assert.assertTrue(tree.containsKey(cluster));
        Assert.assertTrue(tree.get(cluster).containsKey(service));
        Assert.assertTrue(tree.get(cluster).get(service).containsKey(database));
        Assert.assertTrue(tree.get(cluster).get(service).get(database).containsKey(schema1));
        Assert.assertTrue(tree.get(cluster).get(service).get(database).get(schema1).containsKey(table1));
        Assert.assertTrue(tree.get(cluster).get(service).get(database).get(schema1).get(table1).containsKey(column1));
        Assert.assertTrue(tree.get(cluster).get(service).get(database).get(schema1).get(table1).containsKey(column2));

        Assert.assertTrue(tree.get(cluster).get(service).get(database).containsKey(schema2));
        Assert.assertTrue(tree.get(cluster).get(service).get(database).get(schema2).containsKey(table3));
        Assert.assertTrue(tree.get(cluster).get(service).get(database).get(schema2).get(table3).containsKey(column5));
        Assert.assertTrue(tree.get(cluster).get(service).get(database).get(schema2).get(table3).containsKey(column6));
    }

    @Test
    public void testUnionHasPath() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "A1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "A2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "A3");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "A4");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "B1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "B2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "B3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "C1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "C2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "C3");
        a1.addEdge("toB", b1);
        a1.addEdge("toB", b2);
        a1.addEdge("toB", b3);
        b1.addEdge("toC", c1);
        b2.addEdge("toC", c2);
        b3.addEdge("toC", c3);

        GraphTraversal<Vertex, Path> traversal = this.sqlgGraph.traversal().V().has("A", "name", "A1")
                .union(
                        __.out("toB").has("name", P.eq("B1")).out("toC"),
                        __.out("toB").has("name", P.eq("B2")).out("toC"))
                .path();
        printTraversalForm(traversal);

        Set<Object> objs = new HashSet<>();
        while (traversal.hasNext()) {
            Path p = traversal.next();
            Assert.assertEquals(3, p.size());
            Object root0 = p.get(0);
            Assert.assertEquals(a1, root0);
            Object child0 = p.get(1);
            Assert.assertEquals("B", ((Vertex) child0).label());
            Object child1 = p.get(2);
            Assert.assertEquals("C", ((Vertex) child1).label());
            objs.add(child0);
            objs.add(child1);
        }
        Assert.assertEquals(4, objs.size());
        Assert.assertTrue(objs.contains(b1));
        Assert.assertTrue(objs.contains(b2));
        Assert.assertTrue(objs.contains(c1));
        Assert.assertTrue(objs.contains(c2));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A")
                .union(
                        __.optional(__.out("toB").has("name", P.eq("B1")).optional(__.out("toC"))),
                        __.optional(__.out("toB").has("name", P.eq("B2")).optional(__.out("toC"))))
                .path();
        printTraversalForm(traversal);
        List<Path> paths = traversal.toList();
        Assert.assertEquals(8, paths.size());
        Assert.assertEquals(2, paths.stream().filter(p -> p.size() == 3).count());
    }

    @Test
    public void testUnionAsPerUMLG() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB");
        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        bb1.addEdge("ab", a1);
        bb2.addEdge("ab", a1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1).in("ab").toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V(a1).union(__.optional(__.out("ab")), __.optional(__.in("ab"))).toList();
        Assert.assertEquals(4, vertices.size());
        vertices = this.sqlgGraph.traversal().V(a1).union(__.out("ab"), __.in("ab")).toList();
        Assert.assertEquals(4, vertices.size());
        vertices = this.sqlgGraph.traversal().V(a1).union(__.out("ab"), __.in("ab")).toList();
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testUnionFailure() {
        loadModern();
        Traversal<Vertex, Map<String, Long>> traversal = this.sqlgGraph.traversal().V().union(
                __.repeat(__.union(
                        __.out("created"),
                        __.in("created"))).times(2),
                __.repeat(__.union(
                        __.in("created"),
                        __.out("created"))).times(2))
                .label().groupCount();
        printTraversalForm(traversal);
        final Map<String, Long> groupCount = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(12l, groupCount.get("software").longValue());
        Assert.assertEquals(20l, groupCount.get("person").longValue());
        Assert.assertEquals(2, groupCount.size());
    }

    @Test
    public void g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX() {
        loadModern();
        Object marko = convertToVertexId("marko");
        Object vadas = convertToVertexId("vadas");
        final Traversal<Vertex, Number> traversal = this.sqlgGraph.traversal().V(marko, vadas)
                .union(
                        __.outE().count(),
                        __.inE().count(),
                        (Traversal) __.outE().values("weight").sum()
                );
        printTraversalForm(traversal);
        checkResults(Arrays.asList(3l, 1.9d, 1l), traversal);
    }

    @Test
    public void g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX() {
        loadModern();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().out("created")
                .<Vertex>union(
                        __.as("project").in("created").has("name", "marko").select("project"),
                        __.as("project").in("created").in("knows").has("name", "marko").select("project")
                ).toList();
        Assert.assertEquals(7, vertices.size());
        Assert.assertEquals(6, vertices.stream().filter(v -> v.value("name").equals("lop")).count());
        Assert.assertEquals(1, vertices.stream().filter(v -> v.value("name").equals("ripple")).count());

        Traversal<Vertex, Map<String, Long>> traversal = (Traversal) this.sqlgGraph.traversal().V().out("created")
                .union(
                        __.as("project").in("created").has("name", "marko").select("project"),
                        __.as("project").in("created").in("knows").has("name", "marko").select("project")
                ).groupCount().by("name");

        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        final Map<String, Long> map = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(1l, map.get("ripple").longValue());
        Assert.assertEquals(6l, map.get("lop").longValue());
    }

}
