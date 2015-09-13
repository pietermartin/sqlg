package org.umlg.sqlg.test.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;

/**
 * Created by pieter on 2015/09/12.
 */
public class JsonTest extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsJson());
    }

    @Test
    public void testJson() throws IOException {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "doc", json);
        this.sqlgGraph.tx().commit();
        JsonNode value = this.sqlgGraph.traversal().V(v1.id()).next().value("doc");
        Assert.assertEquals(json, value);
    }

    @Test
    public void testJson1() throws IOException {
        ObjectMapper objectMapper =  new ObjectMapper();
        String content = "{\"username\":\"robert\",\"posts\":100122,\"emailaddress\":\"robert@omniti.com\"}";
        JsonNode jsonNode = objectMapper.readTree(content);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "doc", jsonNode);
        this.sqlgGraph.tx().commit();
        JsonNode value = this.sqlgGraph.traversal().V(v1.id()).next().value("doc");
        Assert.assertEquals(jsonNode.get("username"), value.get("username"));
        Assert.assertEquals(jsonNode.get("post"), value.get("post"));
        Assert.assertEquals(jsonNode.get("emailaddress"), value.get("emailaddress"));
    }

    @Test
    public void testJsonArray() throws IOException {
        ObjectMapper objectMapper =  new ObjectMapper();
        ArrayNode jsonArray = new ArrayNode(objectMapper.getNodeFactory());
        ObjectNode john = new ObjectNode(objectMapper.getNodeFactory());
        john.put("username", "john");
        ObjectNode pete = new ObjectNode(objectMapper.getNodeFactory());
        pete.put("username", "pete");
        jsonArray.add(john);
        jsonArray.add(pete);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "users", jsonArray);
        this.sqlgGraph.tx().commit();
        JsonNode value = this.sqlgGraph.traversal().V(v1.id()).next().value("users");
        Assert.assertEquals(jsonArray, value);
    }

}
