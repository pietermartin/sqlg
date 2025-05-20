package org.umlg.sqlg.test.inet;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.inet.PGcidr;
import org.umlg.sqlg.inet.PGinet;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;

public class TestCidr extends BaseTest {

    @Test
    public void testCidr() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("CidrTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGCIDR));
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
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "CidrTest", "name", "a", "ip", new PGcidr("23.239.26.161/32"));

        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("CidrTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGcidr pGcidr = inet.value("ip");
            Assert.assertEquals("23.239.26.161/32", pGcidr.toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "CidrTest", "name", "a", "ip", new PGinet("1.x.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "CidrTest", "name", "a", "ip", new PGinet("1.300.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
    }

    @Test
    public void testIcidrIpv6() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("IcidrTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGCIDR));
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
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "IcidrTest", "name", "a", "ip", new PGcidr("2345:0425:2CA1:0000:0000:0567:5673:23b5"));

        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("IcidrTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGcidr pGcidr = inet.value("ip");
            Assert.assertEquals("2345:425:2ca1::567:5673:23b5/128", pGcidr.toString());
        }
    }

    @Test
    public void testIcidrNormalBatchMode() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("IcidrTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGCIDR));
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
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "IcidrTest", "name", "a", "ip", new PGcidr("1.1.1.1"));
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("IcidrTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGcidr pGcidr = inet.value("ip");
            Assert.assertEquals("1.1.1.1/32", pGcidr.toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "IcidrTest", "name", "a", "ip", new PGcidr("1.x.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "IcidrTest", "name", "a", "ip", new PGcidr("1.300.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
    }

    @Test
    public void testIcidrStreamingBatchMode() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("IcidrTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGCIDR));
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
            this.sqlgGraph.streamVertex(T.label, "IcidrTest", "name", "a", "ip", new PGcidr("1.1.1.1"));
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> ips = this.sqlgGraph.traversal().V().hasLabel("IcidrTest").toList();
        Assert.assertEquals(100, ips.size());
        for (Vertex inet : ips) {
            PGcidr pGcidr= inet.value("ip");
            Assert.assertEquals("1.1.1.1/32", pGcidr.toString());
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "IcidrTest", "name", "a", "ip", new PGcidr("1.x.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
        try {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "IcidrTest", "name", "a", "ip", new PGcidr("1.300.1.1"));
            Assert.fail("Expected an exception");
        } catch (Exception e) {
            //noop
        }
    }
}
