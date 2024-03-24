package org.umlg.sqlg.test.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

/**
 * Created by pieter on 2015/09/12.
 */
public class TestJson extends BaseTest {

    @Test
    public void testJson() {
        ObjectNode json = Topology.OBJECT_MAPPER.createObjectNode();
        json.put("username", "john");
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "doc", json);
        this.sqlgGraph.tx().commit();
        JsonNode value = this.sqlgGraph.traversal().V(v1.id()).next().value("doc");
        Assert.assertEquals(json, value);
    }

    @Test
    public void testJson1() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
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
    public void testJsonArray() {
        ArrayNode jsonArray = Topology.OBJECT_MAPPER.createArrayNode();
        ObjectNode john = Topology.OBJECT_MAPPER.createObjectNode();
        john.put("username", "john");
        ObjectNode pete = Topology.OBJECT_MAPPER.createObjectNode();
        pete.put("username", "pete");
        jsonArray.add(john);
        jsonArray.add(pete);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "users", jsonArray);
        this.sqlgGraph.tx().commit();
        JsonNode value = this.sqlgGraph.traversal().V(v1.id()).next().value("users");
        Assert.assertEquals(jsonArray, value);
    }

    @Test
    public void testJsonArraysForObjectNodes() {
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
    }

    @Test
    public void testJsonArraysForArrayNode() {
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
    }

//    @Test
//    public void testJsonExampleFiles() throws IOException, URISyntaxException {
//        ObjectMapper objectMapper = new ObjectMapper();
//        String content1 = new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemResource("jsonExample1.json").toURI())));
//        JsonNode jsonNode1 = objectMapper.readTree(content1);
//        String content2 = new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemResource("jsonExample2.json").toURI())));
//        JsonNode jsonNode2 = objectMapper.readTree(content2);
//        String content3 = new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemResource("jsonExample3.json").toURI())));
//        JsonNode jsonNode3 = objectMapper.readTree(content3);
//        String content4 = new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemResource("jsonExample4.json").toURI())));
//        JsonNode jsonNode4 = objectMapper.readTree(content4);
//        String content5 = new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemResource("jsonExample5.json").toURI())));
//        JsonNode jsonNode5 = objectMapper.readTree(content5);
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "json1", jsonNode1, "json2", jsonNode2, "json3", jsonNode3, "json4", jsonNode4, "json5", jsonNode5);
//        this.sqlgGraph.tx().commit();
//        JsonNode value1 = this.sqlgGraph.traversal().V(v1.id()).next().value("json1");
//        JsonNode value2 = this.sqlgGraph.traversal().V(v1.id()).next().value("json2");
//        JsonNode value3 = this.sqlgGraph.traversal().V(v1.id()).next().value("json3");
//        JsonNode value4 = this.sqlgGraph.traversal().V(v1.id()).next().value("json4");
//        JsonNode value5 = this.sqlgGraph.traversal().V(v1.id()).next().value("json5");
//        Assert.assertEquals(jsonNode1, value1);
//        Assert.assertEquals(jsonNode2, value2);
//        Assert.assertEquals(jsonNode3, value3);
//        Assert.assertEquals(jsonNode4, value4);
//        Assert.assertEquals(jsonNode5, value5);
//    }

    @Test
    public void testJsonExampleFilesArrays() throws IOException, URISyntaxException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsJsonArrayValues());
        ObjectMapper objectMapper = new ObjectMapper();

        InputStream jsonExample1InputStream = getClass().getResourceAsStream("/jsonExample1.json");
        Assert.assertNotNull("Failed to find jsonExample1.json", jsonExample1InputStream);
        String content1 = IOUtils.toString(jsonExample1InputStream, Charset.defaultCharset());
        JsonNode jsonNode1 = objectMapper.readTree(content1);

        InputStream jsonExample2InputStream = getClass().getResourceAsStream("/jsonExample2.json");
        Assert.assertNotNull("Failed to find jsonExample2.json", jsonExample2InputStream);
        String content2 = IOUtils.toString(jsonExample2InputStream, Charset.defaultCharset());
        JsonNode jsonNode2 = objectMapper.readTree(content2);

        InputStream jsonExample3InputStream = getClass().getResourceAsStream("/jsonExample3.json");
        Assert.assertNotNull("Failed to find jsonExample3.json", jsonExample3InputStream);
        String content3 = IOUtils.toString(jsonExample3InputStream, Charset.defaultCharset());
        JsonNode jsonNode3 = objectMapper.readTree(content3);

        InputStream jsonExample4InputStream = getClass().getResourceAsStream("/jsonExample4.json");
        Assert.assertNotNull("Failed to find jsonExample4.json", jsonExample4InputStream);
        String content4 = IOUtils.toString(jsonExample4InputStream, Charset.defaultCharset());
        JsonNode jsonNode4 = objectMapper.readTree(content4);

        InputStream jsonExample5InputStream = getClass().getResourceAsStream("/jsonExample5.json");
        Assert.assertNotNull("Failed to find jsonExample5.json", jsonExample5InputStream);
        String content5 = IOUtils.toString(jsonExample5InputStream, Charset.defaultCharset());
        JsonNode jsonNode5 = objectMapper.readTree(content5);
        JsonNode[] jsonNodes = new JsonNode[]{jsonNode1, jsonNode2, jsonNode3, jsonNode4, jsonNode5};
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "jsonArray", jsonNodes);
        this.sqlgGraph.tx().commit();
        JsonNode[] value1 = this.sqlgGraph.traversal().V(v1.id()).next().value("jsonArray");
        Assert.assertArrayEquals(jsonNodes, value1);
    }

}
