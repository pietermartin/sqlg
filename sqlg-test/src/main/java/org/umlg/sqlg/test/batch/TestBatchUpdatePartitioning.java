package org.umlg.sqlg.test.batch;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.PartitionType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/09/08
 */
public class TestBatchUpdatePartitioning extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsPartitioning());
    }

    @Test
    public void updatePartitionedTable() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensurePartitionedVertexLabelExist(
                "A",
                new LinkedHashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("name", PropertyType.STRING);
                    put("other", PropertyType.STRING);
                    put("other2", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "name")),
                PartitionType.LIST,
                "name"
        );
        vertexLabel.ensureListPartitionExists("part1", "'a1'");
        vertexLabel.ensureListPartitionExists("part2", "'a2'");
        vertexLabel.ensureListPartitionExists("part3", "'a3'");
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "uid1", UUID.randomUUID().toString(),
                    "name", "a1", "other", "other" + i, "other2", "other2" + i);
        }
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        int count = 10;
        for (Vertex vertex : vertices) {
            vertex.property("other", "other" + count++);
            if (count % 2 == 0) {
                vertex.property("other2", "other2" + count);
            }
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(10, vertices.size());
        count = 10;
        for (Vertex vertex : vertices) {
            Assert.assertEquals("other" + count++, vertex.value("other"));
        }


    }
}