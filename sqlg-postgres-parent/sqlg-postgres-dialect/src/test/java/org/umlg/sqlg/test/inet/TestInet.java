package org.umlg.sqlg.test.inet;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.inet.PGinet;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;

public class TestInet extends BaseTest {

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
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "InetTest", "name", "a", "ip", new PGinet("1.300.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
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
            StringBuilder ip = new StringBuilder();
            for (int j = 0; j < 4; j++) {
                ip.append(j);
                if (j < 3) {
                    ip.append(".");
                }
            }
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
}
