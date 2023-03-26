package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2015/02/01
 * Time: 11:48 AM
 */
public class TestTraversalPerformance extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTraversalPerformance.class);

//    @Test
//    public void testAddVNotCached() {
//        List<Vertex> vertices = this.sqlgGraph.traversal().addV("animal").property("age", 0).toList();
//        Assert.assertEquals(1, vertices.size());
//    }
//
//
//        Scenario: g_V_out_in_selectXall_a_a_aX_byXunfold_name_foldX
//    Given the empty graph
//    And the graph initializer of
//      """
//      g.addV("A").property("name", "a1").as("a1").
//        addV("A").property("name", "a2").as("a2").
//        addV("A").property("name", "a3").as("a3").
//        addV("B").property("name", "b1").as("b1").
//        addV("B").property("name", "b2").as("b2").
//        addV("B").property("name", "b3").as("b3").
//        addE("ab").from("a1").to("b1").
//        addE("ab").from("a2").to("b2").
//        addE("ab").from("a3").to("b3")
//      """
//    And the traversal of
//      """
//      g.V().as("a").out().as("a").in().as("a").
//        select(Pop.all, "a", "a", "a").
//          by(unfold().values('name').fold())
//      """
//    When iterated to list
//    Then the result should be unordered
//      | result |
//            | m[{"a":["a1","b1","a1"]}] |
//            | m[{"a":["a2","b2","a2"]}] |
//            | m[{"a":["a3","b3","a3"]}] |
//    @Test
//    public void test() {
//        this.sqlgGraph.traversal().addV("A").property("name", "a1").as("a1").iterate();
//        this.sqlgGraph.traversal().addV("A").property("name", "a2").as("a2").iterate();
//        this.sqlgGraph.traversal().addV("A").property("name", "a3").as("a3").iterate();
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.traversal().addV("A").property("name", "a1").as("a1").iterate();
//        Assert.assertEquals(3L, this.sqlgGraph.traversal().V().count().next(), 0L);
//    }
//
    //    @Test
//    public void testHasIdWithin() {
//        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
//            put("col1", PropertyDefinition.of(PropertyType.INTEGER));
//        }});
//        this.sqlgGraph.tx().commit();
//
//        Vertex one = this.sqlgGraph.addVertex(T.label, "A", "col1", 1);
//        Vertex two = this.sqlgGraph.addVertex(T.label, "A", "col1", 2);
//        Vertex three = this.sqlgGraph.addVertex(T.label, "A", "col1", 3);
//        Vertex four = this.sqlgGraph.addVertex(T.label, "A", "col1", 4);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().has(T.id, P.within(one.id(), two.id(), three.id(), four.id())).toList();
//        Assert.assertEquals(4, vertices.size());
//        vertices = this.sqlgGraph.traversal().V().has(T.id, P.within(four.id())).toList();
//        Assert.assertEquals(1, vertices.size());
//
//    }
//
    //    @Test
//    public void g_V_hasId() {
//        loadModern();
//        assertModernGraph(this.sqlgGraph, true, false);
//        GraphTraversalSource g = this.sqlgGraph.traversal();
//
//        Object id = convertToVertexId("marko");
//
//        List<Vertex> traversala2 =  g.V().has(T.id, id).toList();
//        Assert.assertEquals(1, traversala2.size());
//        Assert.assertEquals(convertToVertex(this.sqlgGraph, "marko"), traversala2.get(0));
//
//        traversala2 =  g.V().hasId(id).toList();
//        Assert.assertEquals(1, traversala2.size());
//        Assert.assertEquals(convertToVertex(this.sqlgGraph, "marko"), traversala2.get(0));
//    }
//
//    @Test
//    public void test() {
//        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
//            put("col1", PropertyDefinition.of(PropertyType.INTEGER));
//        }});
//        this.sqlgGraph.tx().commit();
//
//        Vertex one = this.sqlgGraph.addVertex(T.label, "A", "col1", 1);
//        Vertex two= this.sqlgGraph.addVertex(T.label, "A", "col1", 2);
//        Vertex three = this.sqlgGraph.addVertex(T.label, "A", "col1", 3);
//        Vertex four = this.sqlgGraph.addVertex(T.label, "A", "col1", 4);
//        this.sqlgGraph.tx().commit();
//
//        Vertex _one = this.sqlgGraph.traversal().V().hasLabel("A").has("col1", P.eq(1)).next();
//        Assert.assertEquals(one, _one);
//        Vertex _two = this.sqlgGraph.traversal().V().hasLabel("A").has("col1", P.eq(2)).next();
//        Assert.assertEquals(two, _two);
//        Vertex _three = this.sqlgGraph.traversal().V().hasLabel("A").has("col1", P.eq(3)).next();
//        Assert.assertEquals(three, _three);
//        Vertex _four = this.sqlgGraph.traversal().V().hasLabel("A").has("col1", P.eq(4)).next();
//        Assert.assertEquals(four, _four);
//    }
//
    @Test
    public void testSpeedWithLargeSchemaFastQuery1() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Map<String, PropertyDefinition> columns = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            columns.put("property_" + i, PropertyDefinition.of(PropertyType.STRING));
        }
        //Create a large schema, it slows the maps  down
//        int NUMBER_OF_SCHEMA_ELEMENTS = 1_000;
        int NUMBER_OF_SCHEMA_ELEMENTS = 1_0;
        for (int i = 0; i < NUMBER_OF_SCHEMA_ELEMENTS; i++) {
            VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist("Person_" + i, columns);
            VertexLabel dog = this.sqlgGraph.getTopology().ensureVertexLabelExist("Dog_" + i, columns);
            person.ensureEdgeLabelExist("pet_" + i, dog, columns);
            if (i % 100 == 0) {
                this.sqlgGraph.tx().commit();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("done creating schema time taken: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        Map<String, Object> columnValues = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            columnValues.put("property_" + i, "asdasdasd");
        }
        for (int i = 0; i < NUMBER_OF_SCHEMA_ELEMENTS; i++) {
            SqlgVertex person = (SqlgVertex) this.sqlgGraph.addVertex("Person_" + i, columnValues);
            SqlgVertex dog = (SqlgVertex) this.sqlgGraph.addVertex("Dog_" + i, columnValues);
            person.addEdgeWithMap("pet_" + i, dog, columnValues);
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("done inserting data time taken: {}", stopWatch);

        stopWatch.reset();
        stopWatch.start();
        for (int i = 0; i < 300_000; i++) {
            Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person_0").out("pet_0").toList().size());
        }
        stopWatch.stop();
        LOGGER.info("total query time: {}", stopWatch);

    }

////    @Test
//    public void testSpeedWithLargeSchemaFastQuery() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        Map<String, PropertyType> columns = new HashMap<>();
//        for (int i = 0; i < 100; i++) {
//            columns.put("property_" + i, PropertyType.STRING);
//        }
//        //Create a large schema, it slows the maps  down
//        for (int i = 0; i < 1_000; i++) {
//            VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist("Person_" + i, columns);
//            VertexLabel dog = this.sqlgGraph.getTopology().ensureVertexLabelExist("Dog_" + i, columns);
//            person.ensureEdgeLabelExist("pet_" + i, dog, columns);
//            if (i % 100 == 0) {
//                this.sqlgGraph.tx().commit();
//            }
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("done time taken " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//
//        Map<String, Object> columnValues = new HashMap<>();
//        for (int i = 0; i < 100; i++) {
//            columnValues.put("property_" + i, "asdasdasd");
//        }
//        for (int i = 0; i < 1_000; i++) {
//            SqlgVertex person = (SqlgVertex) this.sqlgGraph.addVertex("Person_" + i, columnValues);
//            SqlgVertex dog = (SqlgVertex) this.sqlgGraph.addVertex("Dog_" + i, columnValues);
//            person.addEdgeWithMap("pet_" + i, dog, columnValues);
//        }
//        this.sqlgGraph.tx().commit();
//
//        for (int i = 0; i < 100_000; i++) {
//            Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person_0").out("pet_0").toList().size());
//            stopWatch.stop();
//            System.out.println("query time " + stopWatch.toString());
//            stopWatch.reset();
//            stopWatch.start();
//        }
//        stopWatch.stop();
//        System.out.println("query time " + stopWatch.toString());
//
//    }
//
////    @Test
//    public void testSpeed() throws InterruptedException {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        Map<String, Object> columns = new HashMap<>();
//        for (int i = 0; i < 100; i++) {
//            columns.put("property_" + i, "asdasd");
//        }
//        //Create a large schema, it slows the maps  down
//        this.sqlgGraph.tx().normalBatchModeOn();
//        for (int i = 0; i < 100; i++) {
//            if (i % 100 == 0) {
//                stopWatch.stop();
//                System.out.println("got " + i + " time taken " + stopWatch.toString());
//                stopWatch.reset();
//                stopWatch.start();
//            }
//            Vertex person = this.sqlgGraph.addVertex("Person_" + i, columns);
//            Vertex dog = this.sqlgGraph.addVertex("Dog_" + i, columns);
//            ((SqlgVertex)person).addEdgeWithMap("pet_" + i, dog, columns);
//            this.sqlgGraph.tx().commit();
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("done time taken " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//
//        Thread.sleep(5_000);
//
//        Map<String, PropertyType> properties = new HashMap<>() {{
//           put("name", PropertyType.STRING);
//        }};
//        VertexLabel godVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("God", properties);
//        VertexLabel handVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Hand", properties);
//        VertexLabel fingerVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Finger", properties);
//        godVertexLabel.ensureEdgeLabelExist("hand", handVertexLabel);
//        handVertexLabel.ensureEdgeLabelExist("finger", fingerVertexLabel);
//        this.sqlgGraph.tx().commit();
//
//        stopWatch.stop();
//        System.out.println("done another time taken " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//
//        StopWatch stopWatchAddOnly = new StopWatch();
//        stopWatchAddOnly.start();
//
//        this.sqlgGraph.tx().normalBatchModeOn();
//        for (int i = 1; i < 1_00_001; i++) {
//            Vertex a = this.sqlgGraph.addVertex(T.label, "God", "name", "god" + i);
//            for (int j = 0; j < 2; j++) {
//                Vertex b = this.sqlgGraph.addVertex(T.label, "Hand", "name", "name_" + j);
//                a.addEdge("hand", b);
//                for (int k = 0; k < 5; k++) {
//                    Vertex c = this.sqlgGraph.addVertex(T.label, "Finger", "name", "name_" + k);
//                    Edge e = b.addEdge("finger", c);
//                }
//            }
////            if (i % 500_000 == 0) {
////                this.graph.tx().flush();
////                stopWatch.split();
////                System.out.println(stopWatch.toString());
////                stopWatch.unsplit();
////            }
//        }
//        stopWatchAddOnly.stop();
//        System.out.println("Time for add only : " + stopWatchAddOnly.toString());
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("Time for insert: " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//        for (int i = 0; i < 100; i++) {
//            GraphTraversal<Vertex, Path> traversal = sqlgGraph.traversal().V().hasLabel("God").as("god").out("hand").as("hand").out("finger").as("finger").path();
//            while (traversal.hasNext()) {
//                Path path = traversal.next();
//                List<Object> objects = path.objects();
//                assertEquals(3, objects.size());
//                List<Set<String>> labels = path.labels();
//                assertEquals(3, labels.size());
//            }
//            stopWatch.stop();
//            System.out.println("Time for gremlin: " + stopWatch.toString());
//            stopWatch.reset();
//            stopWatch.start();
//        }
//        stopWatch.stop();
//        System.out.println("Time for gremlin: " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//        for (int i = 0; i < 5; i++) {
//            List<Map<String, Vertex>> traversalMap = sqlgGraph.traversal().V().hasLabel("God").as("god").out("hand").as("hand").out("finger").as("finger").<Vertex>select("god", "hand", "finger").toList();
//            assertEquals(1_000_000, traversalMap.size());
//            stopWatch.stop();
//            System.out.println("Time for gremlin 2: " + stopWatch.toString());
//            stopWatch.reset();
//            stopWatch.start();
//        }
//        stopWatch.stop();
//        System.out.println("Time for gremlin 2: " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//
//        for (int i = 0; i < 20; i++) {
//            Connection connection = sqlgGraph.tx().getConnection();
//            try (Statement statement = connection.createStatement()) {
//                ResultSet resultSet = statement.executeQuery("SELECT\n" +
//                        "\t\"public\".\"V_Finger\".\"ID\" AS \"alias1\",\n" +
//                        "\t\"public\".\"V_Finger\".\"name\" AS \"alias2\",\n" +
//                        "\t \"public\".\"V_God\".\"ID\" AS \"alias3\",\n" +
//                        "\t \"public\".\"V_God\".\"name\" AS \"alias4\",\n" +
//                        "\t \"public\".\"V_Hand\".\"ID\" AS \"alias5\",\n" +
//                        "\t \"public\".\"V_Hand\".\"name\" AS \"alias6\",\n" +
//                        "\t \"public\".\"V_Finger\".\"ID\" AS \"alias7\",\n" +
//                        "\t \"public\".\"V_Finger\".\"name\" AS \"alias8\"\n" +
//                        "FROM\n" +
//                        "\t\"public\".\"V_God\" INNER JOIN\n" +
//                        "\t\"public\".\"E_hand\" ON \"public\".\"V_God\".\"ID\" = \"public\".\"E_hand\".\"public.God__O\" INNER JOIN\n" +
//                        "\t\"public\".\"V_Hand\" ON \"public\".\"E_hand\".\"public.Hand__I\" = \"public\".\"V_Hand\".\"ID\" INNER JOIN\n" +
//                        "\t\"public\".\"E_finger\" ON \"public\".\"V_Hand\".\"ID\" = \"public\".\"E_finger\".\"public.Hand__O\" INNER JOIN\n" +
//                        "\t\"public\".\"V_Finger\" ON \"public\".\"E_finger\".\"public.Finger__I\" = \"public\".\"V_Finger\".\"ID\"");
//                while (resultSet.next()) {
//                    String s1 = resultSet.getString("alias1");
//                    String s2 = resultSet.getString("alias2");
//                    String s3 = resultSet.getString("alias3");
//                    String s4 = resultSet.getString("alias4");
//                    String s5 = resultSet.getString("alias5");
//                    String s6 = resultSet.getString("alias6");
//                    String s7 = resultSet.getString("alias7");
//                    String s8 = resultSet.getString("alias8");
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//            stopWatch.stop();
//            System.out.println("Time for name sql 1: " + stopWatch.toString());
//            stopWatch.reset();
//            stopWatch.start();
//        }
//        stopWatch.stop();
//        stopWatch.reset();
//        for (int i = 0; i < 20; i++) {
//            stopWatch.start();
//            Connection connection = sqlgGraph.tx().getConnection();
//            try (Statement statement = connection.createStatement()) {
//                ResultSet resultSet = statement.executeQuery("SELECT\n" +
//                        "\t\"public\".\"V_Finger\".\"ID\" AS \"alias1\",\n" +
//                        "\t\"public\".\"V_Finger\".\"name\" AS \"alias2\",\n" +
//                        "\t \"public\".\"V_God\".\"ID\" AS \"alias3\",\n" +
//                        "\t \"public\".\"V_God\".\"name\" AS \"alias4\",\n" +
//                        "\t \"public\".\"V_Hand\".\"ID\" AS \"alias5\",\n" +
//                        "\t \"public\".\"V_Hand\".\"name\" AS \"alias6\",\n" +
//                        "\t \"public\".\"V_Finger\".\"ID\" AS \"alias7\",\n" +
//                        "\t \"public\".\"V_Finger\".\"name\" AS \"alias8\"\n" +
//                        "FROM\n" +
//                        "\t\"public\".\"V_God\" INNER JOIN\n" +
//                        "\t\"public\".\"E_hand\" ON \"public\".\"V_God\".\"ID\" = \"public\".\"E_hand\".\"public.God__O\" INNER JOIN\n" +
//                        "\t\"public\".\"V_Hand\" ON \"public\".\"E_hand\".\"public.Hand__I\" = \"public\".\"V_Hand\".\"ID\" INNER JOIN\n" +
//                        "\t\"public\".\"E_finger\" ON \"public\".\"V_Hand\".\"ID\" = \"public\".\"E_finger\".\"public.Hand__O\" INNER JOIN\n" +
//                        "\t\"public\".\"V_Finger\" ON \"public\".\"E_finger\".\"public.Finger__I\" = \"public\".\"V_Finger\".\"ID\"");
//                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//
//
//                while (resultSet.next()) {
//                    Long s1 = resultSet.getLong(1);
//                    if (resultSet.wasNull()) {
//                        System.out.println("");
//                    };
//                    String s2 = resultSet.getString(2);
//                    if (resultSet.wasNull()) {
//                        System.out.println("");
//                    };
//                    Long s3 = resultSet.getLong(3);
//                    if (resultSet.wasNull()) {
//                        System.out.println("");
//                    };
//                    String s4 = resultSet.getString(4);
//                    if (resultSet.wasNull()) {
//                        System.out.println("");
//                    };
//                    Long s5 = resultSet.getLong(5);
//                    if (resultSet.wasNull()) {
//                        System.out.println("");
//                    };
//                    String s6 = resultSet.getString(6);
//                    if (resultSet.wasNull()) {
//                        System.out.println("");
//                    };
//                    Long s7 = resultSet.getLong(7);
//                    if (resultSet.wasNull()) {
//                        System.out.println("");
//                    };
//                    String s8 = resultSet.getString(8);
//                    if (resultSet.wasNull()) {
//                        System.out.println("");
//                    };
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//            stopWatch.stop();
//            System.out.println("Time for index sql 2: " + stopWatch.toString());
//            stopWatch.reset();
//        }
//
//        for (int i = 0; i < 20; i++) {
//            stopWatch.start();
//            Connection connection = sqlgGraph.tx().getConnection();
//            try (Statement statement = connection.createStatement()) {
//                ResultSet resultSet = statement.executeQuery("SELECT\n" +
//                        "\t\"public\".\"V_Finger\".\"ID\" AS \"alias1\",\n" +
//                        "\t\"public\".\"V_Finger\".\"name\" AS \"alias2\",\n" +
//                        "\t \"public\".\"V_God\".\"ID\" AS \"alias3\",\n" +
//                        "\t \"public\".\"V_God\".\"name\" AS \"alias4\",\n" +
//                        "\t \"public\".\"V_Hand\".\"ID\" AS \"alias5\",\n" +
//                        "\t \"public\".\"V_Hand\".\"name\" AS \"alias6\",\n" +
//                        "\t \"public\".\"V_Finger\".\"ID\" AS \"alias7\",\n" +
//                        "\t \"public\".\"V_Finger\".\"name\" AS \"alias8\"\n" +
//                        "FROM\n" +
//                        "\t\"public\".\"V_God\" INNER JOIN\n" +
//                        "\t\"public\".\"E_hand\" ON \"public\".\"V_God\".\"ID\" = \"public\".\"E_hand\".\"public.God__O\" INNER JOIN\n" +
//                        "\t\"public\".\"V_Hand\" ON \"public\".\"E_hand\".\"public.Hand__I\" = \"public\".\"V_Hand\".\"ID\" INNER JOIN\n" +
//                        "\t\"public\".\"E_finger\" ON \"public\".\"V_Hand\".\"ID\" = \"public\".\"E_finger\".\"public.Hand__O\" INNER JOIN\n" +
//                        "\t\"public\".\"V_Finger\" ON \"public\".\"E_finger\".\"public.Finger__I\" = \"public\".\"V_Finger\".\"ID\"");
//                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//
//
//                while (resultSet.next()) {
//                    Object s1 = resultSet.getObject(1);
//                    Object s2 = resultSet.getObject(2);
//                    Object s3 = resultSet.getObject(3);
//                    Object s4 = resultSet.getObject(4);
//                    Object s5 = resultSet.getObject(5);
//                    Object s6 = resultSet.getObject(6);
//                    Object s7 = resultSet.getObject(7);
//                    Object s8 = resultSet.getObject(8);
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//            stopWatch.stop();
//            System.out.println("Time for index sql 3: " + stopWatch.toString());
//            stopWatch.reset();
//        }
////        Assert.assertEquals(100_000, vertexTraversal(a).out().out().count().next().intValue());
//    }

}
