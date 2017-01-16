package org.umlg.sqlg.test.mod;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Date: 2014/07/13
 * Time: 6:51 PM
 */
public class TestRemoveProperty extends BaseTest {

    @Test
    public void testRemoveLocalDateTime() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "createOn", LocalDateTime.now());
        this.sqlgGraph.tx().commit();
        v.property("createOn").remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V(v.id()).next().property("createOn").isPresent());
    }

    @Test
    public void testRemoveLocalDate() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "createOn", LocalDate.now());
        this.sqlgGraph.tx().commit();
        v.property("createOn").remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V(v.id()).next().property("createOn").isPresent());
    }

    @Test
    public void testRemoveLocalTime() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "createOn", LocalTime.now());
        this.sqlgGraph.tx().commit();
        v.property("createOn").remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V(v.id()).next().property("createOn").isPresent());
    }

    @Test
    public void testRemoveJson() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsJson());
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "doc", json);
        this.sqlgGraph.tx().commit();
        v.property("doc").remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.traversal().V(v.id()).next().property("doc").isPresent());
    }

    @Test
    public void testRemoveProperty() {
        Vertex v1 = sqlgGraph.addVertex("name", "marko");
        this.sqlgGraph.tx().commit();
        v1.property("name").remove();
        Assert.assertFalse(v1.property("name").isPresent());
    }

    @Test
    public void testRemoveByteArrayProperty() {
        Vertex v1 = sqlgGraph.addVertex("name", "marko", "ages", new byte[]{1, 2, 3, 4});
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(v1.property("ages").isPresent());
        v1.property("ages").remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(v1.property("ages").isPresent());
    }

    @Test
    public void testRemovePropertyWithPeriod() {
        Vertex v1 = sqlgGraph.addVertex("name", "marko", "test.A", "test");
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(v1.property("test.A").isPresent());
        v1.property("test.A").remove();
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(v1.property("test.A").isPresent());
    }

}
