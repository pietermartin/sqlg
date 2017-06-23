package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.structure.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2015/02/01
 * Time: 11:48 AM
 */
public class TestTraversalPerformance extends BaseTest {

    @Test
    public void testSpeedWithLargeSchemaFastQuery1() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Map<String, PropertyType> columns = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            columns.put("property_" + i, PropertyType.STRING);
        }
        //Create a large schema, it slows the maps  down
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
        System.out.println("done creating schema time taken " + stopWatch.toString());
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
        System.out.println("done inserting data time taken " + stopWatch.toString());

        stopWatch.reset();
        stopWatch.start();
        for (int i = 0; i < 1_000_000; i++) {
            Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person_0").out("pet_0").toList().size());
        }
        stopWatch.stop();
        System.out.println("total query time " + stopWatch.toString());

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
//        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
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
