package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * Date: 2016/12/13
 * Time: 7:35 PM
 */
public class TestBatchGlobalUniqueIndexes extends BaseTest {

    @Test
    public void testVertexUniqueConstraintDeleteBatchMode() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());

        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<String, PropertyType>() {{
            put("namea", PropertyType.STRING);
            put("nameb", PropertyType.STRING);
            put("namec", PropertyType.STRING);
        }});
        Set<PropertyColumn> properties = new HashSet<>(vertexLabel.getProperties().values());
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(properties);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 1; i < 1001; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "namea", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3000, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());

        try {
            this.sqlgGraph.addVertex(T.label, "A", "namea", "a1");
            Assert.fail("GlobalUniqueIndex should prevent this form happening");
        } catch (Exception e) {
            //swallow
            this.sqlgGraph.tx().rollback();
        }
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.traversal().V().hasLabel("A").has("namea", "a1").next();
        a1.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2997, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());

        this.sqlgGraph.addVertex(T.label, "A", "namea", "a1");
        //this time it passes.
        this.sqlgGraph.tx().commit();
        testVertexUniqueConstraintDeleteBatchMode_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testVertexUniqueConstraintDeleteBatchMode_assert(this.sqlgGraph1);
        }
    }

    private void testVertexUniqueConstraintDeleteBatchMode_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(3000, sqlgGraph.globalUniqueIndexes().V().count().next().intValue());
    }

    @Test
    public void testVertexUniqueConstraintUpdateNormalBatchMode() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());

        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<String, PropertyType>() {{
            put("namea", PropertyType.STRING);
            put("nameb", PropertyType.STRING);
            put("namec", PropertyType.STRING);
        }});
        Set<PropertyColumn> properties = new HashSet<>(vertexLabel.getProperties().values());
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(properties);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        a1.property("namea", "aa");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            //this should pass
            this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            Assert.fail("GlobalUniqueIndex should not fire");
        }
        testVertexUniqueConstraintUpdateNormalBatchMode_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testVertexUniqueConstraintUpdateNormalBatchMode_assert(this.sqlgGraph1);
        }
    }

    private void testVertexUniqueConstraintUpdateNormalBatchMode_assert(SqlgGraph sqlgGraph) {
        sqlgGraph.tx().normalBatchModeOn();
        try {
            sqlgGraph.addVertex(T.label, "A", "nameb", "aa");
            sqlgGraph.tx().commit();
            Assert.fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testGlobalUniqueIndexOnEdgeNormalBatchMode() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());

        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        VertexLabel vertexLabelA = this.sqlgGraph.getTopology().ensureVertexLabelExist("A", properties);
        VertexLabel vertexLabelB = this.sqlgGraph.getTopology().ensureVertexLabelExist("B", properties);
        properties.clear();
        properties.put("namea", PropertyType.STRING);
        properties.put("nameb", PropertyType.STRING);
        properties.put("namec", PropertyType.STRING);
        vertexLabelA.ensureEdgeLabelExist("ab", vertexLabelB, properties);
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Collection<PropertyColumn> propertyColumns = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getProperties().values();
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
        this.sqlgGraph.tx().commit();

        Schema globalUniqueIndexSchema = this.sqlgGraph.getTopology().getGlobalUniqueIndexSchema();
        Optional<GlobalUniqueIndex> globalUniqueIndexOptional = globalUniqueIndexSchema.getGlobalUniqueIndex("ab_namea_ab_nameb_ab_namec");
        Assert.assertTrue(globalUniqueIndexOptional.isPresent());

        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getProperty("namea");
        Assert.assertTrue(nameaPropertyColumnOptional.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
        Assert.assertEquals(1, globalUniqueIndices.size());
        GlobalUniqueIndex globalUniqueIndex = globalUniqueIndices.iterator().next();
        Assert.assertEquals("ab_namea_ab_nameb_ab_namec", globalUniqueIndex.getName());

        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Edge edge = a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
            a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
            this.sqlgGraph.tx().commit();
            Assert.fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
        this.sqlgGraph.tx().commit();
        testGlobalUniqueIndexOnEdgeNormalBatchMode_assert(this.sqlgGraph, globalUniqueIndex, edge);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testGlobalUniqueIndexOnEdgeNormalBatchMode_assert(this.sqlgGraph1, globalUniqueIndex, edge);
        }
    }

    private void testGlobalUniqueIndexOnEdgeNormalBatchMode_assert(SqlgGraph sqlgGraph, GlobalUniqueIndex globalUniqueIndex, Edge edge) {
        List<Vertex> globalUniqueIndexVertexes = sqlgGraph.globalUniqueIndexes().V().toList();
        Assert.assertEquals(3, globalUniqueIndexVertexes.size());
        Assert.assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.label().equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName())));
        Assert.assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("a")).count());
        Assert.assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("b")).count());
        Assert.assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("c")).count());
        Assert.assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID).equals(edge.id().toString())));

    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testGlobalUniqueIndexOnVertexNormalBatchMode() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());

        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("namec", PropertyType.STRING);
        properties.put("namea", PropertyType.STRING);
        properties.put("nameb", PropertyType.STRING);
        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", properties);
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Collection<PropertyColumn> propertyColumns = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperties().values();
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
        this.sqlgGraph.tx().commit();

        Schema globalUniqueIndexSchema = this.sqlgGraph.getTopology().getGlobalUniqueIndexSchema();
        Optional<GlobalUniqueIndex> globalUniqueIndexOptional = globalUniqueIndexSchema.getGlobalUniqueIndex("A_namea_A_nameb_A_namec");
        Assert.assertTrue(globalUniqueIndexOptional.isPresent());

        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperty("namea");
        Assert.assertTrue(nameaPropertyColumnOptional.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
        Assert.assertEquals(1, globalUniqueIndices.size());
        GlobalUniqueIndex globalUniqueIndex = globalUniqueIndices.iterator().next();
        Assert.assertEquals("A_namea_A_nameb_A_namec", globalUniqueIndex.getName());

        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.tx().normalBatchModeOn();
            this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
            this.sqlgGraph.tx().commit();
            Assert.fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
        this.sqlgGraph.tx().commit();
        testGlobalUniqueIndexOnVertexNormalBatchMode_assert(this.sqlgGraph, globalUniqueIndex, a);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testGlobalUniqueIndexOnVertexNormalBatchMode_assert(this.sqlgGraph1, globalUniqueIndex, a);
        }
    }

    private void testGlobalUniqueIndexOnVertexNormalBatchMode_assert(SqlgGraph sqlgGraph, GlobalUniqueIndex globalUniqueIndex, Vertex a) {
        List<Vertex> globalUniqueIndexVertexes = sqlgGraph.globalUniqueIndexes().V().toList();
        Assert.assertEquals(6, globalUniqueIndexVertexes.size());
        Assert.assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.label().equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName())));
        Assert.assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.property(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).isPresent() && g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("a")).count());
        Assert.assertEquals(3, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID).equals(a.id().toString())).count());
    }

    @Test
    public void testBatchModeGlobalUniqueIndexOnPropertyThatDoesNotYetExists() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());

        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", properties);
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(vertexLabel.getProperties().values()));
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        a1.property("name", "123");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.tx().normalBatchModeOn();
            a2.property("name", "123");
            this.sqlgGraph.tx().commit();
            Assert.fail("GlobalUniqueIndex should prevent this from happening");
        } catch (Exception e) {
            //swallow
        }
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            try {
                this.sqlgGraph1.tx().normalBatchModeOn();
                a2 = this.sqlgGraph1.traversal().V(a2).next();
                a2.property("name", "123");
                this.sqlgGraph1.tx().commit();
                Assert.fail("GlobalUniqueIndex should prevent this from happening");
            } catch (Exception e) {
                //swallow
            }
        }
    }

    @Test
    public void testBatchModeGlobalUniqueIndexOnPropertyThatDoesNotYetExists2() throws InterruptedException {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());

        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", properties);
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(vertexLabel.getProperties().values()));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        a1.property("name", "123");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
            a2.property("name", "123");
            this.sqlgGraph.tx().commit();
            Assert.fail("GlobalUniqueIndex should prevent this from happening");
        } catch (Exception e) {
            //swallow
        }
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            try {
                Vertex a2 = this.sqlgGraph1.addVertex(T.label, "A");
                a2.property("name", "123");
                this.sqlgGraph1.tx().commit();
                Assert.fail("GlobalUniqueIndex should prevent this from happening");
            } catch (Exception e) {
                //swallow
            }

        }
    }
}
