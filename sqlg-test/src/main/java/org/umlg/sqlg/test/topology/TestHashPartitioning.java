package org.umlg.sqlg.test.topology;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.PartitionType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2021/10/11
 */
public class TestHashPartitioning  extends BaseTest {

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    @Test
    public void testHashPartition() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<>() {{
                    put("uid1", PropertyType.INTEGER);
                    put("uid2", PropertyType.LONG);
                    put("uid3", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(List.of("uid1", "uid2", "uid3")),
                PartitionType.HASH,
                "\"uid1\""
        );
        vertexLabel.ensureListPartitionExists("listPartition1", "'1'");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "uid1", 1, "uid2", 1L, "uid3", "halo1");
        this.sqlgGraph.addVertex(T.label, "A", "uid1", 1, "uid2", 2L, "uid3", "halo1");
        this.sqlgGraph.tx().commit();

    }
}
