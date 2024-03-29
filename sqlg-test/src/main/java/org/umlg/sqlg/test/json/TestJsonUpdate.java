package org.umlg.sqlg.test.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/23
 * Time: 5:33 PM
 */
public class TestJsonUpdate extends BaseTest {

    @Test
    public void testUpdateJson() {
        ObjectNode json = Topology.OBJECT_MAPPER.createObjectNode();
        json.put("username", "john");
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "doc", json);
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        json = Topology.OBJECT_MAPPER.createObjectNode();
        json.put("username", "peter");
        v1.property("doc", json);
        this.sqlgGraph.tx().commit();
        JsonNode value = this.sqlgGraph.traversal().V(v1.id()).next().value("doc");
        assertEquals(json, value);
    }

    @Test
    public void testUpdateJson1() throws IOException {
        ObjectMapper objectMapper =  new ObjectMapper();
        String content = "{\"username\":\"robert\",\"posts\":100122,\"emailaddress\":\"robert@omniti.com\"}";
        JsonNode jsonNode = objectMapper.readTree(content);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "doc", jsonNode);
        this.sqlgGraph.tx().commit();
        JsonNode value = this.sqlgGraph.traversal().V(v1.id()).next().value("doc");
        assertEquals(jsonNode.get("username"), value.get("username"));
        assertEquals(jsonNode.get("post"), value.get("post"));
        assertEquals(jsonNode.get("emailaddress"), value.get("emailaddress"));

        content = "{\"username\":\"peter\",\"posts\":100133,\"emailaddress\":\"peter@omniti.com\"}";
        jsonNode = objectMapper.readTree(content);
        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("doc", jsonNode);
        this.sqlgGraph.tx().commit();
        value = this.sqlgGraph.traversal().V(v1.id()).next().value("doc");
        assertEquals(jsonNode.get("username"), value.get("username"));
        assertEquals(jsonNode.get("post"), value.get("post"));
        assertEquals(jsonNode.get("emailaddress"), value.get("emailaddress"));
    }

    @Test
    public void testUpdateJsonArray() {
        ArrayNode jsonArray = Topology.OBJECT_MAPPER.createArrayNode();
        ObjectNode john = Topology.OBJECT_MAPPER.createObjectNode();
        john.put("username", "john");
        ObjectNode pete = Topology.OBJECT_MAPPER.createObjectNode();
        pete.put("username", "pete");
        jsonArray.add(john);
        jsonArray.add(pete);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "users", jsonArray);
        this.sqlgGraph.tx().commit();

        jsonArray = Topology.OBJECT_MAPPER.createArrayNode();
        john = Topology.OBJECT_MAPPER.createObjectNode();
        john.put("username", "john1");
        pete = Topology.OBJECT_MAPPER.createObjectNode();
        pete.put("username", "pete1");
        jsonArray.add(john);
        jsonArray.add(pete);

        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("users", jsonArray);
        this.sqlgGraph.tx().commit();

        JsonNode value = this.sqlgGraph.traversal().V(v1.id()).next().value("users");
        assertEquals(jsonArray, value);
    }

    @Test
    public void testUpdateJsonArraysForObjectNodes() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsJsonArrayValues());
        ObjectNode json1 = Topology.OBJECT_MAPPER.createObjectNode();
        json1.put("username", "john1");
        ObjectNode json2 = Topology.OBJECT_MAPPER.createObjectNode();
        json2.put("username", "john2");
        ObjectNode[] objectNodes = new ObjectNode[]{json1, json2};

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "docs", objectNodes);
        this.sqlgGraph.tx().commit();
        JsonNode[] value = this.sqlgGraph.traversal().V(v1.id()).next().value("docs");
        Assert.assertArrayEquals(objectNodes, value);

        json1 = Topology.OBJECT_MAPPER.createObjectNode();
        json1.put("username", "john11");
        json2 = Topology.OBJECT_MAPPER.createObjectNode();
        json2.put("username", "john22");
        objectNodes = new ObjectNode[]{json1, json2};

        v1 = this.sqlgGraph.traversal().V(v1).next();
        v1.property("docs", objectNodes);
        this.sqlgGraph.tx().commit();
        value = this.sqlgGraph.traversal().V(v1.id()).next().value("docs");
        Assert.assertArrayEquals(objectNodes, value);

    }

    @Test
    public void testUpdateJsonArraysForArrayNode() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsJsonArrayValues());
        ArrayNode jsonArray1 = Topology.OBJECT_MAPPER.createArrayNode();
        ObjectNode john = Topology.OBJECT_MAPPER.createObjectNode();
        john.put("username", "john");
        ObjectNode pete = Topology.OBJECT_MAPPER.createObjectNode();
        pete.put("username", "pete");
        jsonArray1.add(john);
        jsonArray1.add(pete);

        ArrayNode jsonArray2 = Topology.OBJECT_MAPPER.createArrayNode();
        ObjectNode john2 = Topology.OBJECT_MAPPER.createObjectNode();
        john2.put("username", "john2");
        ObjectNode pete2 = Topology.OBJECT_MAPPER.createObjectNode();
        pete2.put("username", "pete2");
        jsonArray2.add(john2);
        jsonArray2.add(pete2);

        ArrayNode[] arrayNodes = new ArrayNode[]{jsonArray1, jsonArray2};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "docs", arrayNodes);
        this.sqlgGraph.tx().commit();
        JsonNode[] value = this.sqlgGraph.traversal().V(v1.id()).next().value("docs");
        Assert.assertArrayEquals(arrayNodes, value);

        jsonArray1 = Topology.OBJECT_MAPPER.createArrayNode();
        john = Topology.OBJECT_MAPPER.createObjectNode();
        john.put("username", "john");
        pete = Topology.OBJECT_MAPPER.createObjectNode();
        pete.put("username", "pete");
        jsonArray1.add(john);
        jsonArray1.add(pete);

        jsonArray2 = Topology.OBJECT_MAPPER.createArrayNode();
        john2 = Topology.OBJECT_MAPPER.createObjectNode();
        john2.put("username", "john2");
        pete2 = Topology.OBJECT_MAPPER.createObjectNode();
        pete2.put("username", "pete2");
        jsonArray2.add(john2);
        jsonArray2.add(pete2);

        arrayNodes = new ArrayNode[]{jsonArray1, jsonArray2};

        v1 = this.sqlgGraph.traversal().V(v1.id()).next();
        v1.property("docs", arrayNodes);
        this.sqlgGraph.tx().commit();

        value = this.sqlgGraph.traversal().V(v1.id()).next().value("docs");
        Assert.assertArrayEquals(arrayNodes, value);
    }
}
