package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Date: 2016/12/03
 * Time: 9:08 PM
 */
public class TestGlobalUniqueIndex extends BaseTest {

    @Test
    public void testGlobalUniqueIndexOnVertex() {
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
        assertTrue(globalUniqueIndexOptional.isPresent());

        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperty("namea");
        assertTrue(nameaPropertyColumnOptional.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
        assertEquals(1, globalUniqueIndices.size());
        GlobalUniqueIndex globalUniqueIndex = globalUniqueIndices.iterator().next();
        assertEquals("A_namea_A_nameb_A_namec", globalUniqueIndex.getName());

        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();
        Vertex aa = this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
        this.sqlgGraph.tx().commit();

        List<Vertex> globalUniqueIndexVertexes = this.sqlgGraph.globalUniqueIndexes().V().toList();
        //This is 6 because all PropertyColumns with a GlobalUniqueIndex gets a value. If no value is present then a null is inserted as the value.
        //null do not partake in the unique constraint so all is well
        assertEquals(6, globalUniqueIndexVertexes.size());
        assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.label().equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName())));
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.property(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).isPresent() && g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("a")).count());
        assertEquals(3, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID).equals(a.id().toString())).count());
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.property(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).isPresent() && g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("aa")).count());
        assertEquals(3, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID).equals(aa.id().toString())).count());
    }

    @Test
    public void testGlobalIndexAcrossMultipleVertexLabels() {
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("a", PropertyType.STRING);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", properties);
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B", properties);
        Set<PropertyColumn> globalUniqueIndexProperties = new HashSet<>();
        globalUniqueIndexProperties.add(aVertexLabel.getProperty("a").get());
        globalUniqueIndexProperties.add(bVertexLabel.getProperty("a").get());
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(globalUniqueIndexProperties);
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "a", "123");
            this.sqlgGraph.tx().commit();
            this.sqlgGraph.addVertex(T.label, "B", "a", "123");
            fail("GlobalUniqueIndex should prevent this from happening");
        } catch (Exception e) {
            //swallow
        }
    }

    @Test
    public void testGlobalUniqueIndexOnVertexNormalBatchMode() {
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
        assertTrue(globalUniqueIndexOptional.isPresent());

        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperty("namea");
        assertTrue(nameaPropertyColumnOptional.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
        assertEquals(1, globalUniqueIndices.size());
        GlobalUniqueIndex globalUniqueIndex = globalUniqueIndices.iterator().next();
        assertEquals("A_namea_A_nameb_A_namec", globalUniqueIndex.getName());

        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.tx().normalBatchModeOn();
            this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
            this.sqlgGraph.tx().commit();
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
        this.sqlgGraph.tx().commit();

        List<Vertex> globalUniqueIndexVertexes = this.sqlgGraph.globalUniqueIndexes().V().toList();
        assertEquals(6, globalUniqueIndexVertexes.size());
        assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.label().equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName())));
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.property(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).isPresent() && g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("a")).count());
        assertEquals(3, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID).equals(a.id().toString())).count());
    }

    @Test
    public void testBatchModeGlobalUniqueIndexOnPropertyThatDoesNotYetExists() {
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
        assertEquals(1, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.tx().normalBatchModeOn();
            a2.property("name", "123");
            this.sqlgGraph.tx().commit();
            fail("GlobalUniqueIndex should prevent this from happening");
        } catch (Exception e) {
            //swallow
        }
    }

    @Test
    public void testBatchModeGlobalUniqueIndexOnPropertyThatDoesNotYetExists2() {
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
        assertEquals(1, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        try {
            a2.property("name", "123");
            this.sqlgGraph.tx().commit();
            fail("GlobalUniqueIndex should prevent this from happening");
        } catch (Exception e) {
            //swallow
        }
    }

    @Test
    public void testGlobalUniqueIndexOnEdge() {
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
        GlobalUniqueIndex globalUniqueIndex = this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
        assertNotNull(globalUniqueIndex);
        this.sqlgGraph.tx().commit();

        Schema globalUniqueIndexSchema = this.sqlgGraph.getTopology().getGlobalUniqueIndexSchema();
        Optional<GlobalUniqueIndex> globalUniqueIndexOptional = globalUniqueIndexSchema.getGlobalUniqueIndex(globalUniqueIndex.getName());
        assertTrue(globalUniqueIndexOptional.isPresent());

        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getProperty("namea");
        assertTrue(nameaPropertyColumnOptional.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
        assertEquals(1, globalUniqueIndices.size());
        assertEquals(globalUniqueIndex, globalUniqueIndices.iterator().next());
        assertEquals("ab_namea_ab_nameb_ab_namec", globalUniqueIndex.getName());

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Edge edge = a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
        this.sqlgGraph.tx().commit();
        try {
            a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
            a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
        this.sqlgGraph.tx().commit();

        List<Vertex> globalUniqueIndexVertexes = this.sqlgGraph.globalUniqueIndexes().V().toList();
        assertEquals(3, globalUniqueIndexVertexes.size());
        assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.label().equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName())));
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("a")).count());
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("b")).count());
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("c")).count());
        assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID).equals(edge.id().toString())));
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testGlobalUniqueIndexAcrossDifferentEdges() {
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("a", PropertyType.STRING);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", properties);
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B", properties);
        EdgeLabel abEdgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, properties);
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("C", properties);
        VertexLabel dVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("D", properties);
        EdgeLabel cdEdgeLabel = cVertexLabel.ensureEdgeLabelExist("cd", dVertexLabel, properties);

        PropertyColumn abEdgeProperty = abEdgeLabel.getProperty("a").get();
        PropertyColumn cdEdgeProperty = cdEdgeLabel.getProperty("a").get();
        Set<PropertyColumn> globalUniqueIndexProperties = new HashSet<>();
        globalUniqueIndexProperties.add(abEdgeProperty);
        globalUniqueIndexProperties.add(cdEdgeProperty);
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(globalUniqueIndexProperties);
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "a", "132");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "a", "132");
        a1.addEdge("ab", b1, "a", "123");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "a", "132");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "a", "132");
        try {
            c1.addEdge("cd", d1, "a", "123");
            fail("GlobalUniqueIndex should prevent this from happening");
        } catch (Exception e) {
            //swallow
        }
    }

    @Test
    public void testGlobalUniqueIndexOnEdgeNormalBatchMode() {
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
        assertTrue(globalUniqueIndexOptional.isPresent());

        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getProperty("namea");
        assertTrue(nameaPropertyColumnOptional.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
        assertEquals(1, globalUniqueIndices.size());
        GlobalUniqueIndex globalUniqueIndex = globalUniqueIndices.iterator().next();
        assertEquals("ab_namea_ab_nameb_ab_namec", globalUniqueIndex.getName());

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
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
        this.sqlgGraph.tx().commit();

        List<Vertex> globalUniqueIndexVertexes = this.sqlgGraph.globalUniqueIndexes().V().toList();
        assertEquals(3, globalUniqueIndexVertexes.size());
        assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.label().equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName())));
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("a")).count());
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("b")).count());
        assertEquals(1, globalUniqueIndexVertexes.stream().filter(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE).equals("c")).count());
        assertTrue(globalUniqueIndexVertexes.stream().allMatch(g -> g.<String>value(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID).equals(edge.id().toString())));
    }

    @Test
    public void testVertexUniqueConstraintUpdate() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<String, PropertyType>() {{
            put("namea", PropertyType.STRING);
            put("nameb", PropertyType.STRING);
            put("namec", PropertyType.STRING);
        }});
        Set<PropertyColumn> properties = new HashSet<>(vertexLabel.getProperties().values());
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(properties);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
        this.sqlgGraph.tx().commit();

        a1.property("namea", "aa");
        this.sqlgGraph.tx().commit();
        try {
            //this should pass
            this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
        } catch (Exception e) {
            fail("GlobalUniqueIndex should not fire");
        }
        try {
            this.sqlgGraph.addVertex(T.label, "A", "nameb", "aa");
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
    }

    @Test
    public void testVertexUniqueConstraintUpdateNormalBatchMode() {
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
            fail("GlobalUniqueIndex should not fire");
        }
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            this.sqlgGraph.addVertex(T.label, "A", "nameb", "aa");
            this.sqlgGraph.tx().commit();
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
    }

    @Test
    public void testEdgeUniqueConstraintUpdate() {
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
        assertTrue(globalUniqueIndexOptional.isPresent());

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Edge e = a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
        this.sqlgGraph.tx().commit();

        e.property("namea", "aa");
        this.sqlgGraph.tx().commit();
        try {
            //this should pass
            a1.addEdge("ab", b1, "namea", "a", "nameb", "bb", "namec", "cc");
        } catch (Exception ex) {
            fail("GlobalUniqueIndex should not fire");
        }
        try {
            a1.addEdge("ab", b1, "namea", "aa", "nameb", "bb", "namec", "cc");
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception ex) {
            //swallow
        }
    }

    @Test
    public void testVertexUniqueConstraintDelete() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<String, PropertyType>() {{
            put("namea", PropertyType.STRING);
            put("nameb", PropertyType.STRING);
            put("namec", PropertyType.STRING);
        }});
        Set<PropertyColumn> properties = new HashSet<>(vertexLabel.getProperties().values());
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(properties);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "namea", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "namea", "a2");
        this.sqlgGraph.tx().commit();

        assertEquals(6, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());

        try {
            Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "namea", "a1");
            fail("GlobalUniqueIndex should prevent this form happening");
        } catch (Exception e) {
            //swallow
            this.sqlgGraph.tx().rollback();
        }
        a1.remove();
        this.sqlgGraph.tx().commit();
        assertEquals(3, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());


        this.sqlgGraph.addVertex(T.label, "A", "namea", "a1");
        //this time it passes.
        this.sqlgGraph.tx().commit();
        assertEquals(6, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());
    }

    @Test
    public void testEdgeUniqueConstraintDelete() {
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("namea", PropertyType.STRING);
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", properties);
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B", properties);
        EdgeLabel edgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, properties);

        Set<PropertyColumn> propertyColumns = new HashSet<>(edgeLabel.getProperties().values());
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(propertyColumns);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "namea", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "namea", "a2");
        Edge edge = a1.addEdge("ab", a2, "namea", "123");
        this.sqlgGraph.tx().commit();

        assertEquals(1, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());

        try {
            a1.addEdge("ab", a2, "namea", "123");
            fail("GlobalUniqueIndex should prevent this form happening");
        } catch (Exception e) {
            //swallow
            this.sqlgGraph.tx().rollback();
        }
        edge.remove();
        this.sqlgGraph.tx().commit();

        assertEquals(0, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());

        a1.addEdge("ab", a2, "namea", "123");
        //this time it passes.
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());
    }

    @Test
    public void testVertexUniqueConstraintDeleteBatchMode() {
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
        assertEquals(3000, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());

        try {
            this.sqlgGraph.addVertex(T.label, "A", "namea", "a1");
            fail("GlobalUniqueIndex should prevent this form happening");
        } catch (Exception e) {
            //swallow
            this.sqlgGraph.tx().rollback();
        }
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.traversal().V().hasLabel("A").has("namea", "a1").next();
        a1.remove();
        this.sqlgGraph.tx().commit();
        assertEquals(2997, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());

        this.sqlgGraph.addVertex(T.label, "A", "namea", "a1");
        //this time it passes.
        this.sqlgGraph.tx().commit();
        assertEquals(3000, this.sqlgGraph.globalUniqueIndexes().V().count().next().intValue());
    }

    //Lukas's tests
    @Test
    public void testVertexSingleLabelUniqueConstraint() throws Exception {
        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", properties);
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(personVertexLabel.getProperties().values()));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "Person", "name", "Joe");
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "Joe");
            fail("Should not have been possible to add 2 people with the same name.");
        } catch (Exception e) {
            //good
            this.sqlgGraph.tx().rollback();
        }
    }

    @Test
    public void testVertexMultiLabelUniqueConstraint() throws Exception {
        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        VertexLabel chocolateVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Chocolate", properties);
        VertexLabel candyVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Candy", properties);
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Icecream", properties);
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<PropertyColumn>() {{
            add(chocolateVertexLabel.getProperty("name").get());
            add(candyVertexLabel.getProperty("name").get());
        }});
        this.sqlgGraph.addVertex(T.label, "Chocolate", "name", "Yummy");
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "Candy", "name", "Yummy");
            fail("A chocolate and a candy should not have the same name.");
        } catch (Exception e) {
            //good
            this.sqlgGraph.tx().rollback();
        }

        this.sqlgGraph.addVertex(T.label, "Icecream", "name", "Yummy");
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testVertexMultipleConstraintsOnSingleProperty() throws Exception {
        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        VertexLabel chocolateVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Chocolate", properties);
        VertexLabel candyVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Candy", properties);
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Icecream", properties);
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<PropertyColumn>() {{
            add(chocolateVertexLabel.getProperty("name").get());
        }});
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<PropertyColumn>() {{
            add(candyVertexLabel.getProperty("name").get());
        }});

        this.sqlgGraph.addVertex(T.label, "Chocolate", "name", "Yummy");
        this.sqlgGraph.addVertex(T.label, "Candy", "name", "Yummy");
        this.sqlgGraph.addVertex(T.label, "Icecream", "name", "Yummy");
        this.sqlgGraph.addVertex(T.label, "Icecream", "name", "Yummy");
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "Chocolate", "name", "Yummy");
            fail("Two chocolates should not have the same name.");
        } catch (Exception e) {
            //good
            this.sqlgGraph.tx().rollback();
        }

        try {
            this.sqlgGraph.addVertex(T.label, "Candy", "name", "Yummy");
            fail("Two candies should not have the same name.");
        } catch (Exception e) {
            //good
            this.sqlgGraph.tx().rollback();
        }
    }

    @Test
    public void testUpdateUniqueProperty() throws Exception {

        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", properties);
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<PropertyColumn>() {{
            add(personVertexLabel.getProperty("name").get());
        }});
        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.property("name", "Joseph");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "Joe");
        this.sqlgGraph.tx().commit();
        v2 = this.sqlgGraph.traversal().V(v2.id()).next();
        try {
            v2.property("name", "Joseph");
            fail("Should not be able to call a person a pre-existing name.");
        } catch (Exception e) {
            //good
        }
    }

    @Test
    public void testDeleteUniqueProperty() throws Exception {
        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
        }};
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", properties);
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(personVertexLabel.getProperties().values()));
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "Joseph");
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "Joseph");
            fail("Should not be able to call a person a pre-existing name.");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
            //good
        }
        this.sqlgGraph.traversal().V(v.id()).next().remove();
        //this time it passes
        this.sqlgGraph.addVertex(T.label, "Person", "name", "Joseph");
    }

}
