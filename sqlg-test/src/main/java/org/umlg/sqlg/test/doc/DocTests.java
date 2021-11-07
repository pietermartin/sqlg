package org.umlg.sqlg.test.doc;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Date: 2016/12/26
 * Time: 9:54 PM
 */
public class DocTests extends BaseTest {

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
    public void showStreamingWithLockBulkEdgeCreation() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int count = 0;
        for (int i = 1; i <= 10; i++) {
            List<Vertex> persons = new ArrayList<>();
            this.sqlgGraph.tx().streamingWithLockBatchModeOn();
            for (int j = 1; j <= 1_000_000; j++) {
                Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John" + count);
                persons.add(person);
            }
            this.sqlgGraph.tx().flush();
            List<Vertex> cars = new ArrayList<>();
            for (int j = 1; j <= 1_000_000; j++) {
                Vertex car = this.sqlgGraph.addVertex(T.label, "Car", "name", "Dodge" + count++);
                cars.add(car);
            }
            this.sqlgGraph.tx().flush();
            Iterator<Vertex> carIter = cars.iterator();
            for (Vertex person : persons) {
                person.addEdge("drives", carIter.next());
            }
            this.sqlgGraph.tx().commit();
        }
        stopWatch.stop();
        System.out.println("Time taken: " + stopWatch.toString());
    }
//
//        @Test
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
//        System.out.println("Time taken: " + stopWatch.toString());
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
//        //flushing is needed before starting streaming Car. Only only one label/table can stream at a time.
//        this.sqlgGraph.tx().flush();
//        for (int i = 1; i <= 10_000_000; i++) {
//            this.sqlgGraph.streamVertex(T.label, "Car", "name", "Dodge" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());
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
//        System.out.println("Time taken: " + stopWatch.toString());
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
//                .emit().times(2).repeat(__.out("lives", "capital"))
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
//                .order().by("name", Order.incr).by("surname", Order.decr)
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
//        VertexLabel personVertexLabel = topology.ensureVertexLabelExist("public", "person", new HashMap<String, PropertyType>() {{
//            put("name", PropertyType.STRING);
//            put("age", PropertyType.INTEGER);
//        }});
//        VertexLabel softwareVertexLabel = topology.ensureVertexLabelExist("public", "software", new HashMap<String, PropertyType>() {{
//            put("name", PropertyType.STRING);
//            put("lang", PropertyType.STRING);
//        }});
//        EdgeLabel createdEdgeLabel = personVertexLabel.ensureEdgeLabelExist("created", softwareVertexLabel, new HashMap<String, PropertyType>() {{
//            put("weight", PropertyType.DOUBLE);
//        }});
//        EdgeLabel knowsEdgeLabel = personVertexLabel.ensureEdgeLabelExist("knows", personVertexLabel, new HashMap<String, PropertyType>() {{
//            put("weight", PropertyType.DOUBLE);
//        }});
//        this.sqlgGraph.tx().commit();
//    }
//
//    @Test
//    public void generalTopologyCreationWithSchema() {
//        Schema schema = this.sqlgGraph.getTopology().ensureSchemaExist("Humans");
//        VertexLabel personVertexLabel = schema.ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
//            put("name", PropertyType.STRING);
//            put("date", PropertyType.LOCALDATE);
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
//        final GraphReader gryoReader = GryoReader.build().create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/org/apache/tinkerpop/gremlin/structure/io/gryo/tinkerpop-modern.kryo")) {
//            gryoReader.readGraph(stream, this.sqlgGraph);
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
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
//            put("name", PropertyType.STRING);
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
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
//            put("firstName", PropertyType.STRING);
//            put("lastName", PropertyType.STRING);
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
//    public void testPersonAndDogDoNotHaveTheSameName() {
//        Map<String, PropertyType> properties = new HashMap<String, PropertyType>() {{
//            put("name", PropertyType.STRING);
//        }};
//        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", properties);
//        VertexLabel dogVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Dog", properties);
//        PropertyColumn personName = personVertexLabel.getProperty("name").get();
//        PropertyColumn dogName = dogVertexLabel.getProperty("name").get();
//        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<PropertyColumn>() {{
//            add(personName);
//            add(dogName);
//        }});
//        this.sqlgGraph.tx().commit();
//
//        this.sqlgGraph.addVertex(T.label, "Person", "name", "Tyson");
//        try {
//            //This will fail
//            this.sqlgGraph.addVertex(T.label, "Dog", "name", "Tyson");
//            fail("Duplicate key violation suppose to prevent this from executing");
//        } catch (RuntimeException e) {
//            //swallow
//            this.sqlgGraph.tx().rollback();
//        }
//    }
}
