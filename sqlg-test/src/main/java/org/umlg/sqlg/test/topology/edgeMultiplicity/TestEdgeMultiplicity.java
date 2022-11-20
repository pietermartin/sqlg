package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;

import static org.junit.Assert.assertTrue;

public class TestEdgeMultiplicity extends BaseTest {

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
//                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
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
//                Assert.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint"));
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

//    @Test
//    public void testMultiplicity() {
//        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
//        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
//        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
//        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
//                EdgeDefinition.of(
//                        Multiplicity.of(1, 1),
//                        Multiplicity.of(5, 5)
//                )
//        );
//        VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
//        aVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel,
//                EdgeDefinition.of(
//                        Multiplicity.of(1, 1),
//                        Multiplicity.of(0, -1)
//                )
//        );
//
//        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
//        VertexLabel aaVertexLabel = aSchema.ensureVertexLabelExist("A");
//        VertexLabel bbVertexLabel = aSchema.ensureVertexLabelExist("B");
//        aaVertexLabel.ensureEdgeLabelExist("ab",
//                bbVertexLabel,
//                EdgeDefinition.of(
//                        Multiplicity.of(1, 1),
//                        Multiplicity.of(1, 1))
//        );
//        this.sqlgGraph.tx().commit();
//
//        Optional<EdgeLabel> abEdgeLabelOptional = aVertexLabel.getOutEdgeLabel("ab");
//        Assert.assertTrue(abEdgeLabelOptional.isPresent());
//        EdgeLabel abEdgeLabel = abEdgeLabelOptional.get();
//        Set<EdgeRole> inEdgeRoles = abEdgeLabel.getInEdgeRoles();
//        Assert.assertEquals(2, inEdgeRoles.size());
//        Set<EdgeRole> outEdgeRoles = abEdgeLabel.getOutEdgeRoles();
//        Assert.assertEquals(1, outEdgeRoles.size());
//        Assert.assertEquals(0, abEdgeLabel.getProperties().size());
//        EdgeRole outEdgeRoleForVertexLabel = abEdgeLabel.getOutEdgeRoles(aVertexLabel);
//        Assert.assertNotNull(outEdgeRoleForVertexLabel);
//        Assert.assertEquals(Multiplicity.of(1, 1), outEdgeRoleForVertexLabel.getMultiplicity());
//        EdgeRole inEdgeRoleForVertexLabel = abEdgeLabel.getInEdgeRoles(bVertexLabel);
//        Assert.assertNotNull(inEdgeRoleForVertexLabel);
//        Assert.assertEquals(Multiplicity.of(5, 5), inEdgeRoleForVertexLabel.getMultiplicity());
//    }
//
//    @Test
//    public void testLoadMultiplicity() {
//        try (SqlgGraph sqlgGraph2 = SqlgGraph.open(configuration)) {
//            VertexLabel aVertexLabel = sqlgGraph2.getTopology().getPublicSchema().ensureVertexLabelExist("A");
//            VertexLabel bVertexLabel = sqlgGraph2.getTopology().getPublicSchema().ensureVertexLabelExist("B");
//            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
//                    EdgeDefinition.of(
//                            Multiplicity.of(5, 5),
//                            Multiplicity.of(5, 5))
//            );
//            sqlgGraph2.tx().commit();
//        }
//        try (SqlgGraph sqlgGraph2 = SqlgGraph.open(configuration)) {
//            Optional<VertexLabel> optionalAVertexLabel = sqlgGraph2.getTopology().getPublicSchema().getVertexLabel("A");
//            Assert.assertTrue(optionalAVertexLabel.isPresent());
//            Optional<VertexLabel> optionalBVertexLabel = sqlgGraph2.getTopology().getPublicSchema().getVertexLabel("B");
//            Assert.assertTrue(optionalBVertexLabel.isPresent());
//            Optional<EdgeLabel> optionalEdgeLabel = sqlgGraph2.getTopology().getPublicSchema().getEdgeLabel("ab");
//            Assert.assertTrue(optionalEdgeLabel.isPresent());
//            EdgeRole edgeRole = optionalEdgeLabel.get().getOutEdgeRoles(optionalAVertexLabel.get());
//            Assert.assertNotNull(edgeRole);
//            Multiplicity multiplicity = edgeRole.getMultiplicity();
//            Assert.assertEquals(Multiplicity.of(5, 5), multiplicity);
//            edgeRole = optionalEdgeLabel.get().getInEdgeRoles(optionalBVertexLabel.get());
//            Assert.assertNotNull(edgeRole);
//            Assert.assertEquals(Multiplicity.of(5, 5), multiplicity);
//        }
//    }

}
