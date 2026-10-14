package org.umlg.sqlg.test.inet;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.inet.PGinet;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class TestInet extends BaseTest {

    @Test
    public void testPostgresqlDropsForwardSlash32() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGINET));
        }});
        this.sqlgGraph.tx().commit();
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        properties.put("name", "a");
        PGinet pGinet = new PGinet("10.10.90.34/32");
        properties.put("ip", pGinet);
        Vertex v1 = this.sqlgGraph.addVertex("InetTest", properties);
        this.sqlgGraph.tx().commit();
        Vertex _v1 = this.sqlgGraph.traversal().V(v1).next();
        Assert.assertEquals("10.10.90.34", _v1.<PGinet>value("ip").getValue());

    }

    @Test
    public void testInetWithLinkedHashMap() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGINET));
        }});
        this.sqlgGraph.tx().commit();

        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        properties.put("name", "a");
        properties.put("ip", new PGinet("23.239.26.161/24"));
        Vertex v1 = this.sqlgGraph.addVertex("InetTest", properties);
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        Assert.assertEquals(1, ips.size());
        Vertex ip = ips.get(0);
        Assert.assertEquals("a", ip.value("name"));
        Assert.assertEquals("23.239.26.161/24", ip.value("ip").toString());
    }

    @Test
    public void testInet() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGINET));
        }});
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 100; i++) {
            StringBuilder ip = new StringBuilder();
            for (int j = 0; j < 4; j++) {
                ip.append(j);
                if (j < 3) {
                    ip.append(".");
                }
            }
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("23.239.26.161/24"));

        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGinet pGinet = inet.value("ip");
            Assert.assertEquals("23.239.26.161/24", pGinet.toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.x.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.300.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
    }

    @Test
    public void testInetArray() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ips", PropertyDefinition.of(PropertyType.PGINET_ARRAY, Multiplicity.of(1, -1)));
        }});
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 100; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(
                    T.label, "InetTest",
                    "name", "a",
                    "ips", new PGinet[]{new PGinet("23.239.26.161/24"), new PGinet("23.239.26.162/24")}
            );

        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGinet[] pGinets = inet.value("ips");
            Assert.assertEquals(2, pGinets.length);
            Assert.assertEquals("23.239.26.161/24", pGinets[0].toString());
            Assert.assertEquals("23.239.26.162/24", pGinets[1].toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ips", new PGinet[]{new PGinet("1.x.1.1")});
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
            Assert.assertTrue(e.getMessage().contains("invalid input syntax for type inet"));
            this.sqlgGraph.tx().rollback();
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ips", new PGinet[]{new PGinet("1.300.1.1")});
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
            Assert.assertTrue(e.getMessage().contains("invalid input syntax for type inet"));
        }
    }

    @Test
    public void testInetIpv6() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGINET));
        }});
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 100; i++) {
            StringBuilder ip = new StringBuilder();
            for (int j = 0; j < 4; j++) {
                ip.append(j);
                if (j < 3) {
                    ip.append(".");
                }
            }
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("2345:0425:2CA1:0000:0000:0567:5673:23b5"));

        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGinet pGinet = inet.value("ip");
            Assert.assertEquals("2345:425:2ca1::567:5673:23b5", pGinet.toString());
        }
    }

    @Test
    public void testInetNormalBatchMode() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGINET));
        }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            StringBuilder ip = new StringBuilder();
            for (int j = 0; j < 4; j++) {
                ip.append(j);
                if (j < 3) {
                    ip.append(".");
                }
            }
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.1.1.1"));
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGinet pGinet = inet.value("ip");
            Assert.assertEquals("1.1.1.1", pGinet.toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.x.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
            this.sqlgGraph.tx().rollback();
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.300.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
            this.sqlgGraph.tx().rollback();
        }

        ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").has("ip", new PGinet("1.1.1.1")).toList();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (Vertex ip : ips) {
            ip.property("ip", new PGinet("1.1.1.2"));
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100, ips.size());
        ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").has("ip", new PGinet("1.1.1.2")).toList();
        Assert.assertEquals(100, ips.size());
    }

    @Test
    public void testInetArrayNormalBatchMode() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ips", PropertyDefinition.of(PropertyType.PGINET_ARRAY));
        }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(
                    T.label, "InetTest",
                    "name", "a",
                    "ips", new PGinet[]{new PGinet("1.1.1.1"), new PGinet("1.1.1.2")}
            );
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGinet[] pGinets = inet.value("ips");
            Assert.assertEquals(2, pGinets.length);
            Assert.assertEquals("1.1.1.1", pGinets[0].toString());
            Assert.assertEquals("1.1.1.2", pGinets[1].toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ips", new PGinet[]{new PGinet("1.x.1.1")});
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
            Assert.assertTrue(e.getMessage().contains("invalid input syntax for type inet"));
            sqlgGraph.tx().rollback();
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ips", new PGinet[]{new PGinet("1.300.1.1")});
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
            Assert.assertTrue(e.getMessage().contains("invalid input syntax for type inet"));
            sqlgGraph.tx().rollback();
        }

        ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (Vertex ip : ips) {
            ip.property("ip", new PGinet[] {new PGinet("2.2.2.2"), new PGinet("3.3.3.3")});
        }
        sqlgGraph.tx().commit();
        ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        for (Vertex ip : ips) {
            PGinet[] pGinets = ip.value("ip");
            Assert.assertEquals(2, pGinets.length);
            Assert.assertEquals("2.2.2.2", pGinets[0].toString());
            Assert.assertEquals("3.3.3.3", pGinets[1].toString());
        }
    }

    @Test
    public void testInetStreamingBatchMode() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGINET));
        }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.1.1.1"));
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGinet pGinet = inet.value("ip");
            Assert.assertEquals("1.1.1.1", pGinet.toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.x.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.300.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
    }

    @Test
    public void testAddToLastOctetAgain() {
        PGinet pGinet = new PGinet("10.10.90.0/31");
        Assert.assertEquals("10.10.90.1/31", pGinet.addToLastOctet(1).getValue());
    }

    @Test
    public void testAddToLastOctet() {
        PGinet pGinet = new PGinet("10.10.90.34");
        Assert.assertEquals("10.10.90.44", pGinet.addToLastOctet(10).getValue());
        Assert.assertEquals("10.10.90.34", pGinet.getValue());

        PGinet pGinetWithCidr = new PGinet("10.10.90.34/24");
        Assert.assertEquals("10.10.90.44/24", pGinetWithCidr.addToLastOctet(10).getValue());

        Assert.assertEquals("10.10.90.255", new PGinet("10.10.90.250").addToLastOctet(5).getValue());

        try {
            new PGinet("10.10.90.250").addToLastOctet(6);
            Assert.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            //noop
        }

        try {
            new PGinet("2345:0425:2CA1:0000:0000:0567:5673:23b5").addToLastOctet(1);
            Assert.fail("Expected an exception");
        } catch (IllegalStateException e) {
            //noop
        }
    }

    @Test
    public void testSubtractFromLastOctet() {
        PGinet pGinet = new PGinet("10.10.90.34");
        Assert.assertEquals("10.10.90.24", pGinet.subtractFromLastOctet(10).getValue());
        Assert.assertEquals("10.10.90.34", pGinet.getValue());

        PGinet pGinetWithCidr = new PGinet("10.10.90.34/24");
        Assert.assertEquals("10.10.90.24/24", pGinetWithCidr.subtractFromLastOctet(10).getValue());

        Assert.assertEquals("10.10.90.0", new PGinet("10.10.90.5").subtractFromLastOctet(5).getValue());

        try {
            new PGinet("10.10.90.5").subtractFromLastOctet(6);
            Assert.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            //noop
        }

        try {
            new PGinet("2345:0425:2CA1:0000:0000:0567:5673:23b5").subtractFromLastOctet(1);
            Assert.fail("Expected an exception");
        } catch (IllegalStateException e) {
            //noop
        }
    }

    @Test
    public void testInetArrayStreamingBatchMode() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ips", PropertyDefinition.of(PropertyType.PGINET_ARRAY));
        }});
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex(
                    T.label, "InetTest",
                    "name", "a",
                    "ips", new PGinet[]{new PGinet("1.1.1.1"), new PGinet("1.1.1.2")}
            );
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("InetTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGinet[] pGinets = inet.value("ips");
            Assert.assertEquals(2, pGinets.length);
            Assert.assertEquals("1.1.1.1", pGinets[0].toString());
            Assert.assertEquals("1.1.1.2", pGinets[1].toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.x.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
            Assert.assertTrue(e.getMessage().contains("invalid input syntax for type inet"));
            sqlgGraph.tx().rollback();
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.300.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
            Assert.assertTrue(e.getMessage().contains("invalid input syntax for type inet"));
        }
    }
}
