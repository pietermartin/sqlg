package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/11/16
 */
public class TestTopologyGraph extends BaseTest {

    @Test
    public void testTopologyGraphVersion() {
        List<Vertex> vertices = this.sqlgGraph.topology().V()
                .hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_GRAPH)
                .toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex = vertices.get(0);
        String versionTmp = "";
        Properties prop = new Properties();
        try {
            prop.load(ClassLoader.getSystemResource("sqlg.application.properties").openStream());
            versionTmp = (String) prop.get("application.version");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(versionTmp, vertex.value("version"));
    }
}
