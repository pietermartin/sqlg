package org.umlg.sqlg.gremlin.server;

import io.netty.buffer.ByteBufAllocator;

public class TestSqlgGremlinServer {

    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private SqlgGremlinServer sqlgGremlinServer;

//    @Before
//    public void before() throws Exception {
//        SqlgGraph sqlgGraph = SqlgGraph.open("src/test/resources/conf/sqlg.properties");
//        SqlgUtil.dropDb(sqlgGraph);
//        sqlgGraph.tx().commit();
//        sqlgGraph.close();
//        this.sqlgGremlinServer = new SqlgGremlinServer();
//        InputStream is = getClass().getClassLoader().getResourceAsStream("conf/gremlin-server-min.yaml");
//        this.sqlgGremlinServer.start(is);
//    }
//
//    @After
//    public void after() {
//        this.sqlgGremlinServer.stop();
//    }
//
//    @Test
//    public void testRecordIdGraphBinarySerializer() throws SerializationException {
//        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
//                TypeSerializerRegistry.build().addCustomType(RecordId.class, new RecordIdBinarySerializer()).create()
//        );
//        SchemaTable schemaTable = SchemaTable.of("schemaA", "tableA");
//        final RecordId recordId = RecordId.from(schemaTable, 333L);
//        final ByteBuf serialized = serializer.serializeResponseAsBinary(
//                ResponseMessage.build(UUID.randomUUID()).result(recordId).create(),
//                allocator
//        );
//        final ResponseMessage deserialized = serializer.deserializeResponse(serialized);
//        final RecordId actual = (RecordId) deserialized.getResult().getData();
//        Assert.assertEquals(recordId, actual);
//    }
//
//    @Test
//    public void testGraphBinarySerializer() {
//        IoRegistry registry = SqlgIoRegistryV3.instance();
//        TypeSerializerRegistry typeSerializerRegistry = TypeSerializerRegistry.build().addRegistry(registry).create();
//        GraphBinaryMessageSerializerV1 graphBinaryMessageSerializerV1 = new GraphBinaryMessageSerializerV1(typeSerializerRegistry);
//        Cluster cluster = Cluster.build().
//                serializer(graphBinaryMessageSerializerV1).
//                create();
//        Client client = cluster.connect();
//
//        GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(client, "g"));
//        g.addV("Person").property("name", "John").iterate();
//        List<Vertex> list = g.V().hasLabel("Person").toList();
//        Assert.assertEquals(1, list.size());
//        List<Map<Object, String>> valueMaps = g.V().hasLabel("Person").<String>valueMap().by(__.unfold()).toList();
//        Assert.assertEquals(1, valueMaps.size());
//        Assert.assertEquals("John", valueMaps.get(0).get("name"));
//    }
//
//    @Test
//    public void testGraphJsonSerializer() {
//        IoRegistry registry = SqlgIoRegistryV3.instance();
//        GraphSONMapper.Builder builder = GraphSONMapper.build().addRegistry(registry).addCustomModule(GraphSONXModuleV3d0.build().create(false));
//        GraphSONMessageSerializerV3d0 graphSONMessageSerializerV3d0 = new GraphSONMessageSerializerV3d0(builder);
//        Cluster cluster = Cluster.build().
//                serializer(graphSONMessageSerializerV3d0).
//                create();
//        Client client = cluster.connect();
//
//        GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(client, "g"));
//        for (int i = 0; i < 10; i++) {
//            g.addV("Person").property("name", "John").iterate();
//        }
//        List<Vertex> persons = g.V().hasLabel("Person").toList();
//        Assert.assertEquals(10, persons.size());
//    }
}
