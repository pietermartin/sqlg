package org.umlg.sqlg.test.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.BatchManager;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/09
 * Time: 9:20 PM
 */
public class TestBatchJson extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (configuration.getString("jdbc.url").contains("postgresql")) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testJson() throws InterruptedException {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "doc", json);
        this.sqlgGraph.tx().commit();
        testJson_assert(this.sqlgGraph, json, a1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testJson_assert(this.sqlgGraph1, json, a1);
        }
    }

    private void testJson_assert(SqlgGraph sqlgGraph, ObjectNode json, Vertex a1) {
        assertEquals(json, sqlgGraph.traversal().V(a1).values("doc").next());
    }

    @Test
    public void batchJson() throws InterruptedException {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "doc", json);
        }
        this.sqlgGraph.tx().commit();
        batchJson_assert(this.sqlgGraph, json);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            batchJson_assert(this.sqlgGraph, json);
        }
    }

    private void batchJson_assert(SqlgGraph sqlgGraph, ObjectNode json) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        JsonNode value = vertices.get(0).value("doc");
        assertEquals(json, value);
    }

    @Test
    public void batchUpdateJson() throws InterruptedException {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "doc", json);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        JsonNode value = vertices.get(0).value("doc");
        assertEquals(json, value);
        this.sqlgGraph.tx().normalBatchModeOn();
        json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "pete");
        for (Vertex vertex : vertices) {
            vertex.property("doc", json);
        }
        this.sqlgGraph.tx().commit();
        batchUpdateJson_assert(this.sqlgGraph, json);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            batchUpdateJson_assert(this.sqlgGraph1, json);
        }
    }

    private void batchUpdateJson_assert(SqlgGraph sqlgGraph, ObjectNode json) {
        List<Vertex> vertices;
        JsonNode value;
        vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        value = vertices.get(0).value("doc");
        assertEquals(json, value);
    }

    @Test
    public void batchUpdateJsonWithNulls() throws InterruptedException {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "Person", "doc1", json);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "Person", "doc2", json);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "Person", "doc3", json);
        this.sqlgGraph.tx().commit();

        ObjectNode jsonAgain = new ObjectNode(objectMapper.getNodeFactory());
        jsonAgain.put("surname", "zzz");
        this.sqlgGraph.tx().normalBatchModeOn();
        a1.property("doc1", jsonAgain);
        a2.property("doc2", jsonAgain);
        a3.property("doc3", jsonAgain);
        this.sqlgGraph.tx().commit();

        batchUpdateJsonWithNulls_assert(this.sqlgGraph, a1, a2, a3, jsonAgain);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            batchUpdateJsonWithNulls_assert(this.sqlgGraph1, a1, a2, a3, jsonAgain);
        }
    }

    private void batchUpdateJsonWithNulls_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2, Vertex a3, ObjectNode jsonAgain) {
        a1 = sqlgGraph.traversal().V(a1.id()).next();
        a2 = sqlgGraph.traversal().V(a2.id()).next();
        a3 = sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertEquals(jsonAgain, a1.value("doc1"));
        Assert.assertFalse(a1.property("doc2").isPresent());
        Assert.assertFalse(a1.property("doc3").isPresent());

        Assert.assertFalse(a2.property("doc1").isPresent());
        Assert.assertEquals(jsonAgain, a2.value("doc2"));
        Assert.assertFalse(a2.property("doc3").isPresent());

        Assert.assertFalse(a3.property("doc1").isPresent());
        Assert.assertFalse(a3.property("doc2").isPresent());
        Assert.assertEquals(jsonAgain, a3.value("doc3"));
    }

    @Test
    public void testBatchJsonContainingEmbeddedJson() throws IOException, InterruptedException {
        String jsonQuery = "{" +
                "\"chartEnabled\":true," +
                "\"geom\":\"{\\\"type\\\":\\\"LineString\\\"," +
                "\\\"coordinates\\\":[[29.86946571,-24.77036915],[29.8698364927907,-24.7697827794629],[29.8690949272093,-24.7697827794629]]}\"," +
                "\"id\":\"2\"}}";
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(jsonQuery);
        keyValues.put("serializedReport", json);
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.STREAMING);
        this.sqlgGraph.streamVertex("Test", keyValues);
        this.sqlgGraph.tx().commit();
        testBatchJsonContainingEmbeddedJson_assert(this.sqlgGraph, json);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBatchJsonContainingEmbeddedJson_assert(this.sqlgGraph1, json);
        }
    }

    private void testBatchJsonContainingEmbeddedJson_assert(SqlgGraph sqlgGraph, JsonNode json) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Test").toList();
        assertEquals(1, vertices.size());
        JsonNode jsonNodeAgain = vertices.get(0).value("serializedReport");
        assertEquals(json, jsonNodeAgain);
    }

}
