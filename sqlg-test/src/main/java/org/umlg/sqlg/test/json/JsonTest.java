package org.umlg.sqlg.test.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        String value = this.sqlgGraph.traversal().V(v1.id()).next().value("doc");
        Assert.assertEquals("{\"username\": \"john\"}", value);
    }

    @Test
    public void testJson1() throws IOException {
        ObjectMapper objectMapper =  new ObjectMapper();
        String content = "{\"username\":\"robert\",\"posts\":100122,\"emailaddress\":\"robert@omniti.com\"}";
        JsonNode jsonNode = objectMapper.readTree(content);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "doc", jsonNode);
        this.sqlgGraph.tx().commit();
        String value = this.sqlgGraph.traversal().V(v1.id()).next().value("doc");
        JsonNode jsonNode1 = objectMapper.readTree(value);
        Assert.assertEquals(jsonNode.get("username"), jsonNode1.get("username"));
        Assert.assertEquals(jsonNode.get("post"), jsonNode1.get("post"));
        Assert.assertEquals(jsonNode.get("emailaddress"), jsonNode1.get("emailaddress"));
    }

}
