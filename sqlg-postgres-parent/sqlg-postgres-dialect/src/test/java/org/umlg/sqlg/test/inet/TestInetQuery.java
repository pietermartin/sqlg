package org.umlg.sqlg.test.inet;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.inet.PGinet;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class TestInetQuery extends BaseTest {

    @Test
    public void testHasAndOrderBy() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGINET));
        }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        Random r = new Random();
        List<PGinet> pGinets = new ArrayList<>();
        PGinet pGinet = new PGinet("0.0.0.0");
        pGinets.add(pGinet);
        this.sqlgGraph.streamVertex(T.label, "InetTest", "name", "a", "ip", pGinet);
        for (int i = 0; i < 1000; i++) {
            String ip = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
            pGinet = new PGinet(ip);
            pGinets.add(pGinet);
            this.sqlgGraph.streamVertex(T.label, "InetTest", "name", "a", "ip", pGinet);
        }
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1001, this.sqlgGraph.traversal().V().hasLabel("InetTest").count().next().intValue());

        for (PGinet _pGinet : pGinets) {
            List<Vertex> vertices =  this.sqlgGraph.traversal().V().hasLabel("InetTest")
                    .has("ip", _pGinet)
                    .toList();
            Assert.assertEquals(1, vertices.size());
        }

        List<Vertex> vertices =  this.sqlgGraph.traversal().V().hasLabel("InetTest")
                .order().by("ip")
                .toList();
        Assert.assertEquals(1001, vertices.size());
        Assert.assertEquals("0.0.0.0", vertices.get(0).value("ip").toString());

        vertices =  this.sqlgGraph.traversal().V().hasLabel("InetTest")
                .order().by("ip", Order.desc)
                .toList();
        Assert.assertEquals(1001, vertices.size());
        Assert.assertEquals("0.0.0.0", vertices.get(vertices.size() - 1).value("ip").toString());
    }

    @Test
    public void testFunction() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        publicSchema.ensureVertexLabelExist("InetTest", new HashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING));
            put("ip", PropertyDefinition.of(PropertyType.PGINET));
        }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        Random r = new Random();
        PGinet pGinet = new PGinet("0.0.0.0");
        this.sqlgGraph.streamVertex(T.label, "InetTest", "name", "a", "ip", pGinet);
        for (int i = 0; i < 1000; i++) {
            String ip = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
            pGinet = new PGinet(ip);
            this.sqlgGraph.streamVertex(T.label, "InetTest", "name", "a", "ip", pGinet);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1001, this.sqlgGraph.traversal().V().hasLabel("InetTest").count().next().intValue());
        List<Vertex> vertices =  this.sqlgGraph.traversal().V().hasLabel("InetTest")
                .<Vertex>fun("iptest", PropertyType.BOOLEAN, x -> "inet '192.168.1.5' << ip")
                .toList();
        Assert.assertTrue(vertices.stream().noneMatch(v -> v.value("iptest")));
    }
}
