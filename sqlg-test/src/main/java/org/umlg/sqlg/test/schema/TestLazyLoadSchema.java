package org.umlg.sqlg.test.schema;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;

/**
 * Date: 2014/11/03
 * Time: 6:22 PM
 */
public class TestLazyLoadSchema extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLazyLoadTableViaVertexHas() throws Exception {
        //Create a new sqlgGraph
        SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration);
        //Not entirely sure what this is for, else it seems hazelcast has not yet distributed the map
        Thread.sleep(1000);
        //add a vertex in the old, the new should only see it after a commit
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, sqlgGraph1.traversal().V().count().next().intValue());
        Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());
        sqlgGraph1.tx().rollback();
        sqlgGraph1.close();
    }

    @Test
    public void testLazyLoadTableViaVertexHasWithKey() throws Exception {
        //Create a new sqlgGraph
        SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration);
        //Not entirely sure what this is for, else it seems hazelcast has not yet distributed the map
        Thread.sleep(1000);
        //add a vertex in the old, the new should only see it after a commit
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, sqlgGraph1.traversal().V().count().next().intValue());
        Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Person").has("name", "a").count().next().intValue());
        sqlgGraph1.tx().rollback();
        sqlgGraph1.close();
    }

    @Test
    public void testLazyLoadTableViaVertexHasWithKeyMissingColumn() throws Exception {
        //Create a new sqlgGraph
        SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration);
        //Not entirely sure what this is for, else it seems hazelcast has not yet distributed the map
        Thread.sleep(1000);
        //add a vertex in the old, the new should only see it after a commit
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, sqlgGraph1.traversal().V().count().next().intValue());
        Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Person").has("name", "a").count().next().intValue());
        Vertex v11 = sqlgGraph1.traversal().V().has(T.label, "Person").<Vertex>has("name", "a").next();
        Assert.assertFalse(v11.property("surname").isPresent());
        //the next alter will lock if this transaction is still active
        sqlgGraph1.tx().rollback();

        //add column in one
        v1.property("surname", "bbb");
        this.sqlgGraph.tx().commit();

        Vertex v12 = sqlgGraph1.addVertex(T.label, "Person", "surname", "ccc");
        Assert.assertEquals("ccc", v12.value("surname"));
        sqlgGraph1.tx().rollback();
        sqlgGraph1.close();
    }

    //Fails via maven for Hsqldb
    @Test
    public void testLazyLoadTableViaEdgeCreation() throws Exception {
        //Create a new sqlgGraph
        SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration);
        Thread.sleep(1000);
        //add a vertex in the old, the new should only see it after a commit
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
        this.sqlgGraph.tx().commit();
        Vertex v11 = sqlgGraph1.addVertex(T.label, "Person", "surname", "ccc");
        Vertex v12 = sqlgGraph1.addVertex(T.label, "Person", "surname", "ccc");
        sqlgGraph1.tx().commit();

        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();

        v11.addEdge("friend", v12);
        sqlgGraph1.tx().commit();

        Assert.assertEquals(1, vertexTraversal(v11).out("friend").count().next().intValue());
        sqlgGraph1.tx().rollback();
        sqlgGraph1.close();
    }

    @Test
    public void testLazyLoadTableViaEdgesHas() throws Exception {
        //Create a new sqlgGraph
        SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration);
        //Not entirely sure what this is for, else it seems hazelcast has not yet distributed the map
        Thread.sleep(1000);
        //add a vertex in the old, the new should only see it after a commit
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().has(T.label, "friend").count().next().intValue());

        Assert.assertEquals(1, sqlgGraph1.traversal().E().count().next().intValue());
        Assert.assertEquals(1, sqlgGraph1.traversal().E().has(T.label, "friend").count().next().intValue());
        Assert.assertEquals(2, sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());
        sqlgGraph1.tx().rollback();
        sqlgGraph1.close();
    }

    @Test
    public void testLoadSchemaRemembersUncommittedSchemas() throws Exception {
        //Create a new sqlgGraph
        SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration);
        //Not entirely sure what this is for, else it seems hazelcast has not yet distributed the map
        Thread.sleep(1000);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();

        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Animal", "name", "b");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Car", "name", "b");
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().has(T.label, "friend").count().next().intValue());
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, sqlgGraph1.traversal().E().count().next().intValue());
        Assert.assertEquals(1, sqlgGraph1.traversal().E().has(T.label, "friend").count().next().intValue());
        Assert.assertEquals(2, sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());

        sqlgGraph1.tx().rollback();
        sqlgGraph1.close();
    }

}
