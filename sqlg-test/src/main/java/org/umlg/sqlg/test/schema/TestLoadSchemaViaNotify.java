package org.umlg.sqlg.test.schema;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;
import org.umlg.sqlg.test.topology.TestTopologyChangeListener;

import java.net.URL;
import java.util.*;

/**
 * Date: 2014/11/03
 * Time: 6:22 PM
 */
public class TestLoadSchemaViaNotify extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLoadSchemaForeignKeyOutSchemaToPublic() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex person = this.sqlgGraph.addVertex(T.label, "Mammal.Person", "name", "a");
            Vertex car = this.sqlgGraph.addVertex(T.label, "Car", "name", "a");
            person.addEdge("drives", car);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            car = sqlgGraph1.traversal().V(car.id()).next();
            Iterator<Vertex> verticesIter = car.vertices(Direction.IN, "drives");
            int size = IteratorUtils.toList(verticesIter).size();
            Assert.assertEquals(1, size);
        }
    }

    @Test
    public void testLoadSchemaForeignKeyInSchemaToPublic() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
            Vertex car = this.sqlgGraph.addVertex(T.label, "Fleet.Car", "name", "a");
            person.addEdge("drives", car);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1_000);
            car = sqlgGraph1.traversal().V(car.id()).next();
            Iterator<Vertex> verticesIter = car.vertices(Direction.IN, "drives");
            int size = IteratorUtils.toList(verticesIter).size();
            Assert.assertEquals(1, size);
        }
    }

    @Test
    public void testLazyLoadTableViaVertexHas() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            //add a vertex in the old, the new should only see it after a commit
            this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
            this.sqlgGraph.tx().commit();
            //postgresql notify only happens after the commit, need to wait a bit.
            Thread.sleep(1000);
            Assert.assertEquals(1, sqlgGraph1.traversal().V().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());
            Assert.assertEquals("a", sqlgGraph1.traversal().V().has(T.label, "Person").next().value("name"));
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLazyLoadTableViaVertexHasWithKey() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            //add a vertex in the old, the new should only see it after a commit
            this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1000);

            Assert.assertEquals(1, sqlgGraph1.traversal().V().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Person").has("name", "a").count().next().intValue());
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLazyLoadTableViaVertexHasWithKeyMissingColumn() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            //add a vertex in the old, the new should only see it after a commit
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
            this.sqlgGraph.tx().commit();
            Thread.sleep(1000);

            Assert.assertEquals(1, sqlgGraph1.traversal().V().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Person").has("name", "a").count().next().intValue());
            Vertex v11 = sqlgGraph1.traversal().V().has(T.label, "Person").has("name", "a").next();
            Assert.assertFalse(v11.property("surname").isPresent());
            //the next alter will lock if this transaction is still active
            sqlgGraph1.tx().rollback();

            //add column in one
            v1.property("surname", "bbb");
            this.sqlgGraph.tx().commit();

            Thread.sleep(1_000);

            Vertex v12 = sqlgGraph1.addVertex(T.label, "Person", "surname", "ccc");
            Assert.assertEquals("ccc", v12.value("surname"));
            sqlgGraph1.tx().rollback();
        }
    }

    //Fails via maven for Hsqldb
    @Test
    public void testLazyLoadTableViaEdgeCreation() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            //add a vertex in the old, the new should only see it after a commit
            Vertex v1 = null;
            Vertex v2 = null;
            for (int i = 0; i < 3; i++) {
                try {
                    v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
                    v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
                    this.sqlgGraph.tx().commit();
                    break;
                } catch (Exception e) {
                    //retry
                    this.sqlgGraph.tx().rollback();
                }
            }
            Thread.sleep(1000);
            Vertex v11 = null;
            Vertex v12 = null;
            for (int i = 0; i < 3; i++) {
                try {
                    v11 = sqlgGraph1.addVertex(T.label, "Person", "surname", "ccc");
                    v12 = sqlgGraph1.addVertex(T.label, "Person", "surname", "ccc");
                    sqlgGraph1.tx().commit();
                } catch (Exception e) {
                    sqlgGraph1.tx().rollback();
                }
            }

            Assert.assertNotNull(v1);
            for (int i = 0; i < 3; i++) {
                try {
                    v1.addEdge("friend", v2);
                    this.sqlgGraph.tx().commit();
                } catch (Exception e) {
                    this.sqlgGraph.tx().rollback();
                }
            }
            Thread.sleep(1000);

            Assert.assertNotNull(v11);
            v11.addEdge("friend", v12);
            sqlgGraph1.tx().commit();

            Assert.assertEquals(1, vertexTraversal(sqlgGraph1, v11).out("friend").count().next().intValue());
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLazyLoadTableViaEdgesHas() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            //add a vertex in the old, the new should only see it after a commit
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
            v1.addEdge("friend", v2);
            this.sqlgGraph.tx().commit();
            Thread.sleep(1000);

            Assert.assertEquals(1, this.sqlgGraph.traversal().E().has(T.label, "friend").count().next().intValue());

            Assert.assertEquals(1, sqlgGraph1.traversal().E().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().E().has(T.label, "friend").count().next().intValue());
            Assert.assertEquals(2, sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());
            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLoadSchemaRemembersUncommittedSchemas() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
            v1.addEdge("friend", v2);
            this.sqlgGraph.tx().commit();
            this.sqlgGraph.addVertex(T.label, "Animal", "name", "b");
            this.sqlgGraph.addVertex(T.label, "Car", "name", "b");
            Assert.assertEquals(1, this.sqlgGraph.traversal().E().has(T.label, "friend").count().next().intValue());
            this.sqlgGraph.tx().commit();

            //allow time for notification to happen
            Thread.sleep(1000);

            Assert.assertEquals(1, sqlgGraph1.traversal().E().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().E().has(T.label, "friend").count().next().intValue());
            Assert.assertEquals(2, sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());

            sqlgGraph1.tx().rollback();
        }
    }

    @Test
    public void testLoadSchemaEdge() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex personVertex = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
            Vertex dogVertex = this.sqlgGraph.addVertex(T.label, "Dog", "name", "b");
            Edge petEdge = personVertex.addEdge("pet", dogVertex, "test", "this");
            this.sqlgGraph.tx().commit();

            //allow time for notification to happen
            Thread.sleep(1_000);

            Assert.assertEquals(1, sqlgGraph1.traversal().E().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().E().has(T.label, "pet").count().next().intValue());
            Assert.assertEquals("this", sqlgGraph1.traversal().E().has(T.label, "pet").next().value("test"));
            Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Dog").count().next().intValue());

            Assert.assertEquals(dogVertex, sqlgGraph1.traversal().V(personVertex.id()).out("pet").next());
            Assert.assertEquals(personVertex, sqlgGraph1.traversal().V(dogVertex.id()).in("pet").next());
            Assert.assertEquals("a", sqlgGraph1.traversal().V(personVertex.id()).next().<String>value("name"));
            Assert.assertEquals("b", sqlgGraph1.traversal().V(dogVertex.id()).next().<String>value("name"));

            sqlgGraph1.tx().rollback();

            //add a property to the vertex
            personVertex.property("surname", "AAA");
            this.sqlgGraph.tx().commit();

            //allow time for notification to happen
            Thread.sleep(1_000);
            Assert.assertEquals("AAA", sqlgGraph1.traversal().V(personVertex.id()).next().<String>value("surname"));
            sqlgGraph1.tx().rollback();

            //add property to the edge
            petEdge.property("edgeProperty1", "a");
            this.sqlgGraph.tx().commit();

            //allow time for notification to happen
            Thread.sleep(1_000);
            Assert.assertEquals("a", sqlgGraph1.traversal().E(petEdge.id()).next().<String>value("edgeProperty1"));
            sqlgGraph1.tx().rollback();

            //add an edge
            Vertex addressVertex = this.sqlgGraph.addVertex(T.label, "Address", "name", "1 Nowhere");
            personVertex.addEdge("homeAddress", addressVertex);
            this.sqlgGraph.tx().commit();

            //allow time for notification to happen
            Thread.sleep(1_000);

            Assert.assertEquals(2, sqlgGraph1.traversal().E().count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().E().has(T.label, "pet").count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().E().has(T.label, "homeAddress").count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Dog").count().next().intValue());
            Assert.assertEquals(1, sqlgGraph1.traversal().V().has(T.label, "Address").count().next().intValue());

        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void loadIndex() throws Exception {
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {

            Map<String, PropertyType> properties = new HashMap<>();
            properties.put("name", PropertyType.STRING);
            VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", properties);
            vertexLabel.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(vertexLabel.getProperty("name").get()));
            this.sqlgGraph.tx().commit();
            Optional<Index> index = this.sqlgGraph.getTopology().getPublicSchema()
                    .getVertexLabel("A").get()
                    .getIndex(this.sqlgGraph.getSqlDialect()
                            .indexName(
                                    SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A"),
                                    Topology.VERTEX_PREFIX,
                                    Collections.singletonList("name")));
            Assert.assertTrue(index.isPresent());

            //allow time for notification to happen
            Thread.sleep(1_000);
            index = sqlgGraph1.getTopology().getPublicSchema()
                    .getVertexLabel("A").get()
                    .getIndex(this.sqlgGraph.getSqlDialect()
                            .indexName(
                                    SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "A"),
                                    Topology.VERTEX_PREFIX,
                                    Collections.singletonList("name")));
            Assert.assertTrue(index.isPresent());
        }
    }

    @Test
    public void testViaNotifyIsCommitted() throws Exception {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "halo");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "name", "halo");
            a1.addEdge("ab", b1, "name", "asd");
            this.sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("A")
                    .ifPresent(v -> v.ensureIndexExists(IndexType.UNIQUE, Collections.singletonList(v.getProperty("name").orElseThrow())));

            Schema aSchema = this.sqlgGraph.getTopology().getSchema("A").orElseThrow();
            Assert.assertTrue(aSchema.isUncommitted());
            VertexLabel vertexLabel = aSchema.getVertexLabel("A").orElseThrow();
            Assert.assertTrue(vertexLabel.isUncommitted());
            PropertyColumn namePropertyColumn = vertexLabel.getProperty("name").orElseThrow();
            Assert.assertTrue(namePropertyColumn.isUncommitted());
            String indexName = this.sqlgGraph.getSqlDialect().indexName(SchemaTable.of("A", "A"), Topology.VERTEX_PREFIX, Collections.singletonList("name"));
            Index index = vertexLabel.getIndex(indexName).orElseThrow();
            Assert.assertTrue(index.isUncommitted());

            this.sqlgGraph.tx().commit();
            //allow time for notification to happen
            Thread.sleep(1_000);
            aSchema = sqlgGraph1.getTopology().getSchema("A").orElseThrow();
            Assert.assertTrue(aSchema.isCommitted());
            vertexLabel = aSchema.getVertexLabel("A").orElseThrow();
            Assert.assertTrue(vertexLabel.isCommitted());
            namePropertyColumn = vertexLabel.getProperty("name").orElseThrow();
            Assert.assertTrue(namePropertyColumn.isCommitted());
            indexName = sqlgGraph1.getSqlDialect().indexName(SchemaTable.of("A", "A"), Topology.VERTEX_PREFIX, Collections.singletonList("name"));
            index = vertexLabel.getIndex(indexName).orElseThrow();
            Assert.assertTrue(index.isCommitted());
        }
    }

    @Test
    public void testDistributedTopologyListener() throws Exception {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

            TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
            sqlgGraph1.getTopology().registerListener(topologyListenerTest);
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "asda");
            Vertex a2 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "asdasd");
            Edge e1 = a1.addEdge("aa", a2);
            a1.property("surname", "asdasd");
            e1.property("special", "");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "A.B", "name", "asdasd");
            a1.addEdge("aa", b1);

            Schema schema = this.sqlgGraph.getTopology().getSchema("A").orElseThrow();
            VertexLabel aVertexLabel = schema.getVertexLabel("A").orElseThrow();
            EdgeLabel edgeLabel = aVertexLabel.getOutEdgeLabel("aa").orElseThrow();
            VertexLabel bVertexLabel = schema.getVertexLabel("B").orElseThrow();
            Index index = aVertexLabel.ensureIndexExists(IndexType.UNIQUE, new ArrayList<>(aVertexLabel.getProperties().values()));

            this.sqlgGraph.tx().commit();
            //allow time for notification to happen
            Thread.sleep(1_000);

            // we're not getting property notification since we get vertex label notification, these include all properties committed
            Assert.assertEquals(5, topologyListenerTriple.size());

            Assert.assertEquals(schema, topologyListenerTriple.get(0).getLeft());
            Assert.assertNull(topologyListenerTriple.get(0).getMiddle());
            Assert.assertEquals(TopologyChangeAction.CREATE, topologyListenerTriple.get(0).getRight());

            Assert.assertEquals(aVertexLabel, topologyListenerTriple.get(1).getLeft());
            Assert.assertNull(topologyListenerTriple.get(1).getMiddle());
            Assert.assertEquals(TopologyChangeAction.CREATE, topologyListenerTriple.get(1).getRight());
            Map<String, PropertyColumn> props = ((VertexLabel) topologyListenerTriple.get(1).getLeft()).getProperties();
            Assert.assertTrue(props.containsKey("name"));
            Assert.assertTrue(props.containsKey("surname"));

            Assert.assertEquals(index, topologyListenerTriple.get(2).getLeft());
            Assert.assertNull(topologyListenerTriple.get(2).getMiddle());
            Assert.assertEquals(TopologyChangeAction.CREATE, topologyListenerTriple.get(2).getRight());

            Assert.assertEquals(edgeLabel, topologyListenerTriple.get(3).getLeft());
            String s = topologyListenerTriple.get(3).getLeft().toString();
            Assert.assertTrue(s.contains(edgeLabel.getSchema().getName()));
            props = ((EdgeLabel) topologyListenerTriple.get(3).getLeft()).getProperties();
            Assert.assertTrue(props.containsKey("special"));
            Assert.assertNull(topologyListenerTriple.get(3).getMiddle());
            Assert.assertEquals(TopologyChangeAction.CREATE, topologyListenerTriple.get(3).getRight());

            Assert.assertEquals(bVertexLabel, topologyListenerTriple.get(4).getLeft());
            Assert.assertNull(topologyListenerTriple.get(4).getMiddle());
            Assert.assertEquals(TopologyChangeAction.CREATE, topologyListenerTriple.get(4).getRight());
        }
    }
}
