package org.umlg.sqlg.test.doc;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/12/26
 * Time: 9:54 PM
 */
@SuppressWarnings("unused")
public class DocTests extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocTests.class);

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.setProperty("implement.foreign.keys", false);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testUpdatePropertyDefinition() {
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
            put("col1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(0, 1)));
        }});
        this.sqlgGraph.tx().commit();
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        a = this.sqlgGraph.traversal().V().hasLabel("A").tryNext().orElseThrow();
        Assert.assertNull(a.value("col1"));
        a.property("col1", "t");
        this.sqlgGraph.tx().commit();

        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
        PropertyColumn propertyColumn = aVertexLabel.getProperty("col1").orElseThrow();
        propertyColumn.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        this.sqlgGraph.tx().commit();

        try {
            this.sqlgGraph.addVertex(T.label, "A");
            Assert.fail("not null constraint expected");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }

        propertyColumn.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1), "'test'"));
        a = this.sqlgGraph.addVertex(T.label, "A");
        Assert.assertEquals("test", a.value("col1"));
        this.sqlgGraph.tx().commit();

        propertyColumn.updatePropertyDefinition(PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1), "'test'", "(starts_with(" + sqlgGraph.getSqlDialect().maybeWrapInQoutes("col1") + ", 't'))"));

        try {
            a = this.sqlgGraph.addVertex(T.label, "A", "col1", "x");
            Assert.fail("check constraint expected");
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
        }
        a = this.sqlgGraph.addVertex(T.label, "A", "col1", "taaa");
        this.sqlgGraph.tx().commit();
    }

    //    @Test
//    public void showStreamingWithLockBulkEdgeCreation() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        int count = 0;
//        for (int i = 1; i <= 10; i++) {
//            List<Vertex> persons = new ArrayList<>();
//            this.sqlgGraph.tx().streamingWithLockBatchModeOn();
//            for (int j = 1; j <= 1_000_000; j++) {
//                Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John" + count);
//                persons.add(person);
//            }
//            this.sqlgGraph.tx().flush();
//            List<Vertex> cars = new ArrayList<>();
//            for (int j = 1; j <= 1_000_000; j++) {
//                Vertex car = this.sqlgGraph.addVertex(T.label, "Car", "name", "Dodge" + count++);
//                cars.add(car);
//            }
//            this.sqlgGraph.tx().flush();
//            Iterator<Vertex> carIter = cars.iterator();
//            for (Vertex person : persons) {
//                person.addEdge("drives", carIter.next());
//            }
//            this.sqlgGraph.tx().commit();
//        }
//        stopWatch.stop();
//        System.out.println("Time taken: " + stopWatch);
//    }
//
//    @Test
//    public void showBulkEdgeCreation() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        int count = 0;
//        for (int i = 1; i <= 10; i++) {
//            List<Pair<String, String>> identifiers = new ArrayList<>();
//            this.sqlgGraph.tx().streamingBatchModeOn();
//            for (int j = 1; j <= 1_000_000; j++) {
//                this.sqlgGraph.streamVertex(T.label, "Person", "name", "John" + count, "personUid", String.valueOf(count));
//            }
//            this.sqlgGraph.tx().flush();
//            for (int j = 1; j <= 1_000_000; j++) {
//                this.sqlgGraph.streamVertex(T.label, "Car", "name", "Dodge" + count, "carUid", String.valueOf(count));
//                identifiers.add(Pair.of(String.valueOf(count), String.valueOf(count++)));
//            }
//            this.sqlgGraph.tx().flush();
//            this.sqlgGraph.bulkAddEdges("Person", "Car", "drives", Pair.of("personUid", "carUid"), identifiers);
//            this.sqlgGraph.tx().commit();
//        }
//        stopWatch.stop();
//        System.out.println("Time taken: " + stopWatch);
//    }
//
//    @Test
//    public void showStreamingBatchMode() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        //enable streaming mode
//        this.sqlgGraph.tx().streamingBatchModeOn();
//        for (int i = 1; i <= 10_000_000; i++) {
//            this.sqlgGraph.streamVertex(T.label, "Person", "name", "John" + i);
//        }
//        //flushing is needed before starting streaming Car. Only one label/table can stream at a time.
//        this.sqlgGraph.tx().flush();
//        for (int i = 1; i <= 10_000_000; i++) {
//            this.sqlgGraph.streamVertex(T.label, "Car", "name", "Dodge" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println(stopWatch);
//    }
//
//    @Test
//    public void showNormalBatchMode() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        for (int i = 1; i <= 10_000_000; i++) {
//            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John" + i);
//            Vertex car = this.sqlgGraph.addVertex(T.label, "Car", "name", "Dodge" + i);
//            person.addEdge("drives", car);
//            if (i % 100_000 == 0) {
//                this.sqlgGraph.tx().flush();
//            }
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("Time taken: " + stopWatch);
//    }
//
//    @Test
//    public void testLimitOnVertexLabels() {
//        for (int i = 0; i < 100; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "person" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        List<String> names = this.sqlgGraph.traversal()
//                .V().hasLabel("Person")
//                .order().by("name")
//                .limit(3)
//                .<String>values("name")
//                .toList();
//        assertEquals(3, names.size());
//        assertEquals("person0", names.get(0));
//        assertEquals("person1", names.get(1));
//        assertEquals("person10", names.get(2));
//    }
//
//    @Test
//    public void testRangeOnVertexLabels() {
//        for (int i = 0; i < 100; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "person" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        List<String> names = this.sqlgGraph.traversal()
//                .V().hasLabel("Person")
//                .order().by("name")
//                .range(1, 4)
//                .<String>values("name")
//                .toList();
//        assertEquals(3, names.size());
//        assertEquals("person1", names.get(0));
//        assertEquals("person10", names.get(1));
//        assertEquals("person11", names.get(2));
//    }
//
//    @SuppressWarnings("resource")
//    @Test
//    public void testOptionalNested() {
//        Vertex google = this.sqlgGraph.addVertex(T.label, "Company", "name", "Google");
//        Vertex apple = this.sqlgGraph.addVertex(T.label, "Company", "name", "Apple");
//        Vertex usa = this.sqlgGraph.addVertex(T.label, "Country", "name", "USA");
//        Vertex england = this.sqlgGraph.addVertex(T.label, "Country", "name", "England");
//        Vertex newYork = this.sqlgGraph.addVertex(T.label, "City", "name", "New York");
//        google.addEdge("activeIn", usa);
//        google.addEdge("activeIn", england);
//        usa.addEdge("capital", newYork);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal()
//                .V()
//                .hasLabel("Company")
//                .optional(
//                        out().optional(
//                                out()
//                        )
//                )
//                .path()
//                .toList();
//        paths.forEach(p -> System.out.println(p.toString()));
//    }
//
//    @Test
//    public void showRepeat() {
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
//        Vertex peterski = this.sqlgGraph.addVertex(T.label, "Person", "name", "Peterski");
//        Vertex paul = this.sqlgGraph.addVertex(T.label, "Person", "name", "Paul");
//        Vertex usa = this.sqlgGraph.addVertex(T.label, "Country", "name", "USA");
//        Vertex russia = this.sqlgGraph.addVertex(T.label, "Country", "name", "Russia");
//        Vertex washington = this.sqlgGraph.addVertex(T.label, "City", "name", "Washington");
//        john.addEdge("lives", usa);
//        peterski.addEdge("lives", russia);
//        usa.addEdge("capital", washington);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V()
//                .hasLabel("Person")
//                .emit().times(2).repeat(out("lives", "capital"))
//                .path().by("name")
//                .toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//    }
//
//    @Test
//    public void testOrderBy() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "a");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "b");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "c");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "a");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "b");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "c");
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> result = this.sqlgGraph.traversal().V().hasLabel("A")
//                .order().by("name", Order.asc).by("surname", Order.desc)
//                .toList();
//
//        assertEquals(6, result.size());
//        assertEquals(a3, result.get(0));
//        assertEquals(a2, result.get(1));
//        assertEquals(a1, result.get(2));
//        assertEquals(b3, result.get(3));
//        assertEquals(b2, result.get(4));
//        assertEquals(b1, result.get(5));
//    }
//
//    @Test
//    public void showSearchOnLocalDateTime() {
//        LocalDateTime born1 = LocalDateTime.of(1990, 1, 1, 1, 1, 1);
//        LocalDateTime born2 = LocalDateTime.of(1990, 1, 1, 1, 1, 2);
//        LocalDateTime born3 = LocalDateTime.of(1990, 1, 1, 1, 1, 3);
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "born", born1);
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "Peter", "born", born2);
//        Vertex paul = this.sqlgGraph.addVertex(T.label, "Person", "name", "Paul", "born", born3);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Person")
//                .has("born", P.eq(born1))
//                .toList();
//        assertEquals(1, persons.size());
//        assertEquals(john, persons.get(0));
//
//        persons = this.sqlgGraph.traversal().V().hasLabel("Person")
//                .has("born", P.between(LocalDateTime.of(1990, 1, 1, 1, 1, 1), LocalDateTime.of(1990, 1, 1, 1, 1, 3)))
//                .toList();
//        //P.between is inclusive to exclusive
//        assertEquals(2, persons.size());
//        assertTrue(persons.contains(john));
//        assertTrue(persons.contains(peter));
//    }
//
//    @Test
//    public void showTextPredicate() {
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John XXX Doe");
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "Peter YYY Snow");
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> persons = this.sqlgGraph.traversal().V()
//                .hasLabel("Person")
//                .has("name", Text.contains("XXX")).toList();
//
//        assertEquals(1, persons.size());
//        assertEquals(john, persons.get(0));
//    }
//
//    @Test
//    public void showContainsPredicate() {
//        List<Integer> numbers = new ArrayList<>(10000);
//        for (int i = 0; i < 10000; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "number", i);
//            numbers.add(i);
//        }
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> persons = this.sqlgGraph.traversal().V()
//                .hasLabel("Person")
//                .has("number", P.within(numbers))
//                .toList();
//
//        assertEquals(10000, persons.size());
//    }
//
//    @SuppressWarnings("ResultOfMethodCallIgnored")
//    @Test
//    public void showComparePredicate() {
//        Vertex easternUnion = this.sqlgGraph.addVertex(T.label, "Organization", "name", "EasternUnion");
//        Vertex legal = this.sqlgGraph.addVertex(T.label, "Division", "name", "Legal");
//        Vertex dispatch = this.sqlgGraph.addVertex(T.label, "Division", "name", "Dispatch");
//        Vertex newYork = this.sqlgGraph.addVertex(T.label, "Office", "name", "NewYork");
//        Vertex singapore = this.sqlgGraph.addVertex(T.label, "Office", "name", "Singapore");
//        easternUnion.addEdge("organization_division", legal);
//        easternUnion.addEdge("organization_division", dispatch);
//        legal.addEdge("division_office", newYork);
//        dispatch.addEdge("division_office", singapore);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
//                .hasLabel("Organization")
//                .out()
//                .out()
//                .has("name", P.eq("Singapore"));
//        System.out.println(traversal);
//        traversal.hasNext();
//        System.out.println(traversal);
//        List<Vertex> offices = traversal.toList();
//        assertEquals(1, offices.size());
//        assertEquals(singapore, offices.get(0));
//    }
//
//    @SuppressWarnings("ResultOfMethodCallIgnored")
//    @Test
//    public void showHighLatency() {
//        Vertex easternUnion = this.sqlgGraph.addVertex(T.label, "Organization", "name", "EasternUnion");
//        Vertex legal = this.sqlgGraph.addVertex(T.label, "Division", "name", "Legal");
//        Vertex dispatch = this.sqlgGraph.addVertex(T.label, "Division", "name", "Dispatch");
//        Vertex newYork = this.sqlgGraph.addVertex(T.label, "Office", "name", "NewYork");
//        Vertex singapore = this.sqlgGraph.addVertex(T.label, "Office", "name", "Singapore");
//        easternUnion.addEdge("organization_division", legal);
//        easternUnion.addEdge("organization_division", dispatch);
//        legal.addEdge("division_office", newYork);
//        dispatch.addEdge("division_office", singapore);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
//                .hasLabel("Organization")
//                .out()
//                .out();
//        System.out.println(traversal);
//        traversal.hasNext();
//        System.out.println(traversal);
//        List<Vertex> offices = traversal.toList();
//        assertEquals(2, offices.size());
//    }
//
//    @Test
//    public void createModernUpfront() {
//        Topology topology = this.sqlgGraph.getTopology();
//        VertexLabel personVertexLabel = topology.ensureVertexLabelExist("public", "person", new HashMap<>() {{
//            put("name", PropertyDefinition.of(PropertyType.STRING));
//            put("age", PropertyDefinition.of(PropertyType.INTEGER));
//        }});
//        VertexLabel softwareVertexLabel = topology.ensureVertexLabelExist("public", "software", new HashMap<>() {{
//            put("name", PropertyDefinition.of(PropertyType.STRING));
//            put("lang", PropertyDefinition.of(PropertyType.STRING));
//        }});
//        EdgeLabel createdEdgeLabel = personVertexLabel.ensureEdgeLabelExist("created", softwareVertexLabel, new HashMap<>() {{
//            put("weight", PropertyDefinition.of(PropertyType.DOUBLE));
//        }});
//        EdgeLabel knowsEdgeLabel = personVertexLabel.ensureEdgeLabelExist("knows", personVertexLabel, new HashMap<>() {{
//            put("weight", PropertyDefinition.of(PropertyType.DOUBLE));
//        }});
//        this.sqlgGraph.tx().commit();
//    }
//
//    @Test
//    public void generalTopologyCreationWithSchema() {
//        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("Humans");
//        VertexLabel personVertexLabel = schema.ensureVertexLabelExist("Person", new HashMap<>() {{
//            put("name", PropertyDefinition.of(PropertyType.STRING));
//            put("date", PropertyDefinition.of(PropertyType.LOCALDATE));
//        }});
//        this.sqlgGraph.tx().commit();
//    }
//
//    @Test
//    public void queryCache() {
//        loadModern();
//        Optional<Schema> publicSchema = this.sqlgGraph.getTopology().getSchema(this.sqlgGraph.getSqlDialect().getPublicSchema());
//        assertTrue(publicSchema.isPresent());
//        Schema publicSchemaViaShortCut = this.sqlgGraph.getTopology().getPublicSchema();
//        Optional<VertexLabel> personVertexLabel = publicSchemaViaShortCut.getVertexLabel("person");
//        assertTrue(personVertexLabel.isPresent());
//        Optional<EdgeLabel> createEdgeLabel = personVertexLabel.get().getOutEdgeLabel("created");
//        assertTrue(createEdgeLabel.isPresent());
//        Optional<EdgeLabel> knowsEdgeLabel = personVertexLabel.get().getOutEdgeLabel("knows");
//        assertTrue(knowsEdgeLabel.isPresent());
//
//        Optional<PropertyColumn> namePropertyColumn = personVertexLabel.get().getProperty("name");
//        assertTrue(namePropertyColumn.isPresent());
//        assertEquals(PropertyType.STRING, namePropertyColumn.get().getPropertyType());
//        Optional<PropertyColumn> agePropertyColumn = personVertexLabel.get().getProperty("age");
//        assertTrue(agePropertyColumn.isPresent());
//        assertEquals(PropertyType.INTEGER, agePropertyColumn.get().getPropertyType());
//        Optional<PropertyColumn> weightPropertyColumn = createEdgeLabel.get().getProperty("weight");
//        assertTrue(weightPropertyColumn.isPresent());
//        assertEquals(PropertyType.DOUBLE, weightPropertyColumn.get().getPropertyType());
//    }
//
//    @Test
//    public void showTopologyTraversals() {
//        Io.Builder<GraphSONIo> builder = GraphSONIo.build(GraphSONVersion.V3_0);
//        final GraphReader reader = sqlgGraph.io(builder).reader().create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern-v3d0.json")) {
//            reader.readGraph(stream, sqlgGraph);
//        } catch (IOException e) {
//            Assert.fail(e.getMessage());
//        }
//        System.out.println("//All vertex labels");
//        sqlgGraph.topology().V()
//                .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
//                .forEachRemaining(
//                        v -> System.out.println(v.<String>value(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME))
//                );
//
//        System.out.println("//All edge labels");
//        sqlgGraph.topology().V()
//                .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
//                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
//                .forEachRemaining(
//                        v -> System.out.println(v.<String>value(Topology.SQLG_SCHEMA_EDGE_LABEL_NAME))
//                );
//
//        System.out.println("//'person' properties");
//        sqlgGraph.topology().V()
//                .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
//                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "person")
//                .out(Topology.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
//                .forEachRemaining(
//                        v -> {
//                            System.out.print(v.<String>value(Topology.SQLG_SCHEMA_PROPERTY_NAME) + " : ");
//                            System.out.println(v.<String>value(Topology.SQLG_SCHEMA_PROPERTY_TYPE));
//                        }
//                );
//
//        System.out.println("//'software' properties");
//        sqlgGraph.topology().V()
//                .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
//                .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "software")
//                .out(Topology.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
//                .forEachRemaining(
//                        v -> {
//                            System.out.print(v.<String>value(Topology.SQLG_SCHEMA_PROPERTY_NAME) + " : ");
//                            System.out.println(v.<String>value(Topology.SQLG_SCHEMA_PROPERTY_TYPE));
//                        }
//                );
//
//        System.out.println("//'created' properties");
//        sqlgGraph.topology().V()
//                .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
//                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
//                .has(Topology.SQLG_SCHEMA_EDGE_LABEL_NAME, "created")
//                .out(Topology.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
//                .forEachRemaining(
//                        v -> {
//                            System.out.print(v.<String>value(Topology.SQLG_SCHEMA_PROPERTY_NAME) + " : ");
//                            System.out.println(v.<String>value(Topology.SQLG_SCHEMA_PROPERTY_TYPE));
//                        }
//                );
//
//        System.out.println("//'knows' properties");
//        sqlgGraph.topology().V()
//                .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
//                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
//                .has(Topology.SQLG_SCHEMA_EDGE_LABEL_NAME, "knows")
//                .out(Topology.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
//                .forEachRemaining(
//                        v -> {
//                            System.out.print(v.<String>value(Topology.SQLG_SCHEMA_PROPERTY_NAME) + " : ");
//                            System.out.println(v.<String>value(Topology.SQLG_SCHEMA_PROPERTY_TYPE));
//                        }
//                );
//    }
//
//    @Test
//    public void useAsPerNormal() {
//        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
//        Vertex address = this.sqlgGraph.addVertex(T.label, "Address", "street", "13th");
//        person.addEdge("livesAt", address, "since", LocalDate.of(2010, 1, 21));
//        this.sqlgGraph.tx().commit();
//        List<Vertex> addresses = this.sqlgGraph.traversal().V().hasLabel("Person").out("livesAt").toList();
//        assertEquals(1, addresses.size());
//    }
//
//    @Test
//    public void testElementsInSchema() {
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Manager", "name", "john");
//        Vertex palace1 = this.sqlgGraph.addVertex(T.label, "continent.House", "name", "palace1");
//        Vertex corrola = this.sqlgGraph.addVertex(T.label, "fleet.Car", "model", "corrola");
//        palace1.addEdge("managedBy", john);
//        corrola.addEdge("owner", john);
//        this.sqlgGraph.tx().commit();
//        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Manager").count().next().intValue());
//        assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("House").count().next().intValue());
//        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("continent.House").count().next().intValue());
//        assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("Car").count().next().intValue());
//        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("fleet.Car").count().next().intValue());
//        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("managedBy").count().next().intValue());
//        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("owner").count().next().intValue());
//    }
//
//    @Test
//    public void testEdgeAcrossSchema() {
//        Vertex a = this.sqlgGraph.addVertex(T.label, "A.A");
//        Vertex b = this.sqlgGraph.addVertex(T.label, "B.B");
//        Vertex c = this.sqlgGraph.addVertex(T.label, "C.C");
//        a.addEdge("specialEdge", b);
//        b.addEdge("specialEdge", c);
//        this.sqlgGraph.tx().commit();
//        assertEquals(2, this.sqlgGraph.traversal().E().hasLabel("specialEdge").count().next().intValue());
//        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("A.specialEdge").count().next().intValue());
//        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("B.specialEdge").count().next().intValue());
//    }
//
//    @Test
//    public void testIndex() {
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", new HashMap<>() {{
//            put("name", PropertyDefinition.of(PropertyType.STRING));
//        }});
//        Optional<PropertyColumn> namePropertyOptional = personVertexLabel.getProperty("name");
//        assertTrue(namePropertyOptional.isPresent());
//        Index index = personVertexLabel.ensureIndexExists(IndexType.NON_UNIQUE, Collections.singletonList(namePropertyOptional.get()));
//        this.sqlgGraph.tx().commit();
//
//        this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
//        List<Vertex> johns = this.sqlgGraph.traversal().V()
//                .hasLabel("Person")
//                .has("name", "John")
//                .toList();
//        assertEquals(1, johns.size());
//    }
//
//    @Test
//    public void testCompositeIndex() {
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", new HashMap<>() {{
//            put("firstName", PropertyDefinition.of(PropertyType.STRING));
//            put("lastName", PropertyDefinition.of(PropertyType.STRING));
//        }});
//        personVertexLabel.ensureIndexExists(IndexType.NON_UNIQUE, new ArrayList<>(personVertexLabel.getProperties().values()));
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.addVertex(T.label, "Person", "firstName", "John", "lastName", "Smith");
//        List<Vertex> johnSmiths = this.sqlgGraph.traversal().V()
//                .hasLabel("Person")
//                .has("firstName", "John")
//                .has("lastName", "Smith")
//                .toList();
//        assertEquals(1, johnSmiths.size());
//    }
//
//    @Test
//    public void testCheckConstraints() {
//        this.sqlgGraph.getTopology().getPublicSchema()
//                .ensureVertexLabelExist("Person",
//                        new HashMap<>() {{
//                            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(), "'Peter'", "name <> 'John'"));
//                        }}
//                );
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.addVertex(T.label, "Person");
//        this.sqlgGraph.tx().commit();
//        boolean failure = false;
//        try {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
//            this.sqlgGraph.tx().commit();
//        } catch (Exception e) {
//            LOGGER.error(e.getMessage(), e);
//            failure = true;
//        }
//        assertTrue(failure);
//    }
//
//    @Test
//    public void testNameIsRequired() {
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
//                .ensureVertexLabelExist("Person",
//                        new HashMap<>() {{
//                            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                        }}
//                );
//        boolean failure = false;
//        try {
//            this.sqlgGraph.addVertex(T.label, "Person");
//            this.sqlgGraph.tx().commit();
//        } catch (Exception e) {
//            LOGGER.error(e.getMessage(), e);
//            failure = true;
//        }
//        assertTrue(failure);
//    }
//
//    @Test
//    public void testPartitioningRange() {
//        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
//        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist(
//                "Measurement",
//                new LinkedHashMap<>() {{
//                    put("date", PropertyDefinition.of(PropertyType.LOCALDATE));
//                    put("temp", PropertyDefinition.of(PropertyType.INTEGER));
//                }},
//                ListOrderedSet.listOrderedSet(Collections.singletonList("date")),
//                PartitionType.RANGE,
//                "date");
//        partitionedVertexLabel.ensureRangePartitionExists("measurement1", "'2016-07-01'", "'2016-08-01'");
//        partitionedVertexLabel.ensureRangePartitionExists("measurement2", "'2016-08-01'", "'2016-09-01'");
//        this.sqlgGraph.tx().commit();
//
//        LocalDate localDate1 = LocalDate.of(2016, 7, 1);
//        this.sqlgGraph.addVertex(T.label, "Measurement", "date", localDate1);
//        LocalDate localDate2 = LocalDate.of(2016, 8, 1);
//        this.sqlgGraph.addVertex(T.label, "Measurement", "date", localDate2);
//        this.sqlgGraph.tx().commit();
//
//        assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("Measurement").count().next(), 0);
//        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate1).count().next(), 0);
//        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate2).count().next(), 0);
//
//        Partition partition = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("Measurement").get().getPartition("measurement1").get();
//        partition.remove();
//        this.sqlgGraph.tx().commit();
//
//        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").count().next(), 0);
//        assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate1).count().next(), 0);
//        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Measurement").has("date", localDate2).count().next(), 0);
//
//        assertEquals(1, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PARTITION).count().next(), 0);
//    }
//
//    @Test
//    public void testSubPartition() {
//        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
//        VertexLabel partitionedVertexLabel = publicSchema.ensurePartitionedVertexLabelExist(
//                "User",
//                new LinkedHashMap<>() {{
//                    put("username", PropertyDefinition.of(PropertyType.STRING));
//                    put("country", PropertyDefinition.of(PropertyType.STRING));
//                    put("age", PropertyDefinition.of(PropertyType.INTEGER));
//                    put("dateOfBirth", PropertyDefinition.of(PropertyType.LOCALDATE));
//                }},
//                ListOrderedSet.listOrderedSet(List.of("username", "country", "age")),
//                PartitionType.LIST,
//                "country");
//        Partition usa = partitionedVertexLabel.ensureListPartitionWithSubPartitionExists("USA", "'USA'", PartitionType.RANGE, "age");
//        Partition sa = partitionedVertexLabel.ensureListPartitionWithSubPartitionExists("SA", "'SA'", PartitionType.RANGE, "age");
//        Partition gb = partitionedVertexLabel.ensureListPartitionWithSubPartitionExists("GB", "'GB'", PartitionType.RANGE, "age");
//        usa.ensureRangePartitionExists("usa0to10", "0", "10");
//        usa.ensureRangePartitionExists("usa10to20", "10", "20");
//        sa.ensureRangePartitionExists("sa0to10", "0", "10");
//        sa.ensureRangePartitionExists("sa10to20", "10", "20");
//        gb.ensureRangePartitionExists("gb0to10", "0", "10");
//        gb.ensureRangePartitionExists("gb10to20", "10", "20");
//        this.sqlgGraph.tx().commit();
//
//        LocalDate localDate = LocalDate.now();
//        for (int age = 0; age < 20; age++) {
//            for (String country : List.of("USA", "SA", "GB")) {
//                for (String username : List.of("John", "Peter", "David")) {
//                    this.sqlgGraph.addVertex(
//                            T.label, "User",
//                            "username", username,
//                            "country", country,
//                            "age", age,
//                            "dateOfBirth", localDate.minusYears(age)
//                    );
//                }
//            }
//        }
//        this.sqlgGraph.tx().commit();
//        List<Vertex> users = this.sqlgGraph.traversal().V()
//                .hasLabel("User")
//                .has("country", P.eq("USA"))
//                .has("age", 5).toList();
//        assertEquals(3, users.size());
//    }
//
//    @Test
//    public void testOneToOne() {
//        VertexLabel computerVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Computer",
//                new LinkedHashMap<>() {{
//                    put("serialNo", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                }}
//        );
//        VertexLabel cpuVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Cpu",
//                new LinkedHashMap<>() {{
//                    put("serialNo", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                }}
//        );
//        computerVertexLabel.ensureEdgeLabelExist(
//                "cpu",
//                cpuVertexLabel,
//                EdgeDefinition.of(
//                        Multiplicity.of(1, 1),
//                        Multiplicity.of(1, 1)
//                )
//        );
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.getTopology().lock();
//        Vertex computer1 = this.sqlgGraph.addVertex(T.label, "Computer", "serialNo", "1111");
//        Vertex cpu1 = this.sqlgGraph.addVertex(T.label, "Cpu", "serialNo", "aaab");
//        Vertex cpu2 = this.sqlgGraph.addVertex(T.label, "Cpu", "serialNo", "aaac");
//        computer1.addEdge("cpu", cpu1);
//        this.sqlgGraph.tx().commit();
//        try {
//            computer1.addEdge("cpu", cpu2);
//        } catch (RuntimeException e) {
//            if (isPostgres()) {
//                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
//            }
//        }
//        this.sqlgGraph.tx().rollback();
//        Vertex computer2 = this.sqlgGraph.addVertex(T.label, "Computer", "serialNo", "2222");
//        try {
//            computer2.addEdge("cpu", cpu1);
//        } catch (RuntimeException e) {
//            if (isPostgres()) {
//                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
//            }
//        }
//        this.sqlgGraph.tx().rollback();
//    }
//
//    @Test
//    public void testOneToMany() {
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person",
//                new LinkedHashMap<>() {{
//                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                }}
//        );
//        VertexLabel countryVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Country",
//                new LinkedHashMap<>() {{
//                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                }}
//        );
//        personVertexLabel.ensureEdgeLabelExist(
//                "visited",
//                countryVertexLabel,
//                EdgeDefinition.of(
//                        Multiplicity.of(0, 1),
//                        Multiplicity.of(-1, -1, true)
//                )
//        );
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.getTopology().lock();
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
//        Vertex usa = this.sqlgGraph.addVertex(T.label, "Country", "name", "USA");
//        Vertex sa = this.sqlgGraph.addVertex(T.label, "Country", "name", "SA");
//        john.addEdge("visited", usa);
//        john.addEdge("visited", sa);
//        this.sqlgGraph.tx().commit();
//        try {
//            john.addEdge("visited", usa);
//        } catch (RuntimeException e) {
//            if (isPostgres()) {
//                assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
//            }
//        }
//        this.sqlgGraph.tx().rollback();
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
//        peter.addEdge("visited", usa);
//        this.sqlgGraph.tx().commit();
//    }
//
//    @Test
//    public void testUniqueManyToMany() {
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person",
//                new LinkedHashMap<>() {{
//                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                }}
//        );
//        VertexLabel vehicleVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Vehicle",
//                new LinkedHashMap<>() {{
//                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                }}
//        );
//        personVertexLabel.ensureEdgeLabelExist(
//                "drives",
//                vehicleVertexLabel,
//                EdgeDefinition.of(
//                        Multiplicity.of(-1, -1, true),
//                        Multiplicity.of(-1, -1, true)
//                )
//        );
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.getTopology().lock();
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "Peter");
//        Vertex toyota = this.sqlgGraph.addVertex(T.label, "Vehicle", "name", "Toyota");
//        Vertex kia = this.sqlgGraph.addVertex(T.label, "Vehicle", "name", "Kia");
//        john.addEdge("drives", toyota);
//        john.addEdge("drives", kia);
//        peter.addEdge("drives", toyota);
//        peter.addEdge("drives", kia);
//        this.sqlgGraph.tx().commit();
//        try {
//            john.addEdge("drives", toyota);
//        } catch (RuntimeException e) {
//            if (isPostgres()) {
//                assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
//            }
//        }
//        this.sqlgGraph.tx().rollback();
//    }
//
//    @Test
//    public void testCheckMultiplicitiesPerVertex() {
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person",
//                new LinkedHashMap<>() {{
//                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                }}
//        );
//        VertexLabel addressVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Address",
//                new LinkedHashMap<>() {{
//                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
//                }}
//        );
//        EdgeLabel personAddressEdgeLabel = personVertexLabel.ensureEdgeLabelExist(
//                "address",
//                addressVertexLabel,
//                EdgeDefinition.of(
//                        Multiplicity.of(0, 1, true),
//                        Multiplicity.of(1, 3, true)
//                )
//        );
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.getTopology().lock();
//        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
//        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
//        Vertex johnHomeAddress = this.sqlgGraph.addVertex(T.label, "Address", "name", "home");
//        Vertex johnWorkAddress = this.sqlgGraph.addVertex(T.label, "Address", "name", "work");
//        Vertex johnVacationAddress = this.sqlgGraph.addVertex(T.label, "Address", "name", "vacation");
//        try {
//            this.sqlgGraph.tx().checkMultiplicity(john, Direction.OUT, personAddressEdgeLabel, addressVertexLabel);
//        } catch (RuntimeException e) {
//            if (isPostgres()) {
//                assertTrue(e.getMessage().contains("Multiplicity check for EdgeLabel 'address' fails.\n" +
//                        "Lower multiplicity is 1 current lower multiplicity is 0"));
//            }
//        }
//        john.addEdge("address", johnHomeAddress);
//        john.addEdge("address", johnWorkAddress);
//        john.addEdge("address", johnVacationAddress);
//        this.sqlgGraph.tx().checkMultiplicity(john, Direction.OUT, personAddressEdgeLabel, addressVertexLabel);
//
//        peter.addEdge("address", johnHomeAddress);
//        boolean fails = false;
//        try {
//            this.sqlgGraph.tx().checkMultiplicity(johnHomeAddress, Direction.IN, personAddressEdgeLabel, personVertexLabel);
//        } catch (RuntimeException e) {
//            fails = true;
//            if (isPostgres()) {
//                System.out.println(e.getMessage());
//                assertTrue(e.getMessage().contains("Multiplicity check for EdgeLabel 'address' fails.\n" +
//                        "Upper multiplicity is 1 current upper multiplicity is 2"));
//            }
//        }
//        assertTrue(fails);
//        this.sqlgGraph.tx().commit();
//    }

    @Test
    public void testCheckMultiplicitiesPerVertexLabel() {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
        );
        VertexLabel addressVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Address",
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
        );
        EdgeLabel personAddressEdgeLabel = personVertexLabel.ensureEdgeLabelExist(
                "address",
                addressVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, 1, true),
                        Multiplicity.of(1, 3, true)
                )
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "John");
        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        Vertex johnHomeAddress = this.sqlgGraph.addVertex(T.label, "Address", "name", "home");
        Vertex johnWorkAddress = this.sqlgGraph.addVertex(T.label, "Address", "name", "work");
        Vertex johnVacationAddress = this.sqlgGraph.addVertex(T.label, "Address", "name", "vacation");
        john.addEdge("address", johnHomeAddress);
        john.addEdge("address", johnWorkAddress);
        john.addEdge("address", johnVacationAddress);
        peter.addEdge("address", johnHomeAddress);

        this.sqlgGraph.tx().checkMultiplicity(personVertexLabel, Direction.OUT, personAddressEdgeLabel, addressVertexLabel);
        boolean fails = false;
        try {
            this.sqlgGraph.tx().checkMultiplicity(addressVertexLabel, Direction.IN, personAddressEdgeLabel, personVertexLabel);
        } catch (RuntimeException e) {
            fails = true;
            if (isPostgres()) {
                System.out.println(e.getMessage());
                String msg = String.format("Multiplicity check for EdgeLabel 'address' fails for '%s'.\nUpper multiplicity is [1] current multiplicity is [2]", johnHomeAddress.id().toString());
                System.out.println(msg);
                assertTrue(e.getMessage().contains(msg));
            }
        }
        assertTrue(fails);
        this.sqlgGraph.tx().commit();
    }
}
