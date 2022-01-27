package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestTopologyLock extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTopologyLock.class);

    @Test
    public void testTopologyLocked() {
        this.sqlgGraph.getTopology().lock();
        boolean failed = false;
        try {
            this.sqlgGraph.addVertex(T.label, "A");
        } catch (IllegalStateException e) {
            failed = true;
        }
        Assert.assertTrue(failed);
        failed = false;
        this.sqlgGraph.getTopology().unlock();
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertexList = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(1, vertexList.size());
        this.sqlgGraph.getTopology().lock();
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
        try {
            vertexLabel.ensurePropertiesExist(new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
        } catch (IllegalStateException e) {
            failed = true;
        }
        Assert.assertTrue(failed);
        this.sqlgGraph.getTopology().unlock();
        vertexLabel.ensurePropertiesExist(new HashMap<>() {{
            put("a", PropertyType.STRING);
        }});
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();
        this.sqlgGraph.addVertex(T.label, "A", "a", "halo");
        this.sqlgGraph.tx().commit();
        vertexList = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(2, vertexList.size());
    }

    @Test
    public void testTopologyUnlockByTransaction() {
        this.sqlgGraph.getTopology().lock();
        Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
        Assert.assertTrue(this.sqlgGraph.tx().isTopologyLocked());
        this.sqlgGraph.tx().unlockTopology();
        Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
        Assert.assertFalse(this.sqlgGraph.tx().isTopologyLocked());
        this.sqlgGraph.tx().rollback();
        Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
        Assert.assertTrue(this.sqlgGraph.tx().isTopologyLocked());
        this.sqlgGraph.tx().unlockTopology();
        Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
        Assert.assertFalse(this.sqlgGraph.tx().isTopologyLocked());
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
        Assert.assertTrue(this.sqlgGraph.tx().isTopologyLocked());
    }

    @Test
    public void testTopologyMultiThreadUnlockByTransaction() throws InterruptedException {
        this.sqlgGraph.getTopology().lock();
        Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
        Assert.assertTrue(this.sqlgGraph.tx().isTopologyLocked());
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int j = 0; j < 200; j++) {
            int finalJ = j;
            executorService.submit(() -> {
                try {
                    Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
                    Assert.assertTrue(this.sqlgGraph.tx().isTopologyLocked());
                    sqlgGraph.tx().unlockTopology();
                    if (finalJ % 10 == 0) {
                        throw new RuntimeException("blah");
                    }
                    Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
                    Assert.assertFalse(this.sqlgGraph.tx().isTopologyLocked());
                    sqlgGraph.tx().commit();
                    Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
                    Assert.assertTrue(this.sqlgGraph.tx().isTopologyLocked());
                } catch (Exception e) {
                    sqlgGraph.tx().rollback();
                    Assert.assertTrue(this.sqlgGraph.getTopology().isLocked());
                    Assert.assertTrue(this.sqlgGraph.tx().isTopologyLocked());
                }
            });
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(6000, TimeUnit.SECONDS)) {
            Assert.fail("failed to terminate executor service normally");
        }
    }

    @Test
    public void testLockAndCreateSchema() {
        this.sqlgGraph.getTopology().ensureSchemaExist("A");
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").isPresent());
        this.sqlgGraph.getTopology().lock();
        try {
            this.sqlgGraph.getTopology().ensureSchemaExist("B");
            Assert.fail("Expected IllegalStateException");
        } catch (IllegalStateException ignore) {
        }
        this.sqlgGraph.tx().unlockTopology();
        this.sqlgGraph.getTopology().ensureSchemaExist("B");
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("B").isPresent());
        try {
            this.sqlgGraph.getTopology().getSchema("B").orElseThrow().ensureVertexLabelExist("B");
            Assert.fail("Expected IllegalStateException");
        } catch (IllegalStateException ignore) {
        }
        this.sqlgGraph.tx().unlockTopology();
        this.sqlgGraph.getTopology().getSchema("B").orElseThrow().ensureVertexLabelExist("B");
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("B").orElseThrow().getVertexLabel("B").isPresent());
        try {
            this.sqlgGraph.getTopology().getSchema("B").orElseThrow()
                    .getVertexLabel("B").orElseThrow()
                    .ensurePropertiesExist(new HashMap<>() {{
                        put("a", PropertyType.STRING);
                    }});
            Assert.fail("Expected IllegalStateException");
        } catch (IllegalStateException ignore) {
        }
        this.sqlgGraph.tx().unlockTopology();
        this.sqlgGraph.getTopology().getSchema("B").orElseThrow()
                .getVertexLabel("B").orElseThrow()
                .ensurePropertiesExist(new HashMap<>() {{
                    put("a", PropertyType.STRING);
                }});
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("B").orElseThrow().getVertexLabel("B").orElseThrow().getProperty("a").isPresent());
    }

    @Test
    public void topologyGlobalLockUnlock() {
        this.sqlgGraph.getTopology().lock();
        try {
            this.sqlgGraph.addVertex(T.label, "A");
            Assert.fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            //The topology is locked so an IllegalStateException is thrown.
        }
        this.sqlgGraph.getTopology().unlock();
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.getTopology()
                .getPublicSchema()
                .getVertexLabel("A")
                .isPresent());
    }

    @Test
    public void topologyGlobalLockTransactionUnlock() {
        this.sqlgGraph.getTopology().lock();
        this.sqlgGraph.tx().unlockTopology();
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Assert.assertTrue(this.sqlgGraph.getTopology()
                .getPublicSchema()
                .getVertexLabel("A")
                .isPresent());
    }

    @Test
    public void testUnlockTopologyMultiThreaded() throws InterruptedException {
        //Mariadb fails on teamcity with connection not available, some mariadb config setting, passes locally.
        Assume.assumeFalse(isMariaDb());
        this.sqlgGraph.getTopology().lock();
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 200; i++) {
            int finalI = i;
            executorService.submit(() -> {
                try {
                    this.sqlgGraph.tx().unlockTopology();
                    for (int j = 0; j < 10; j++) {
                        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A" + finalI + "_" + j);
                    }
                    this.sqlgGraph.tx().commit();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    Assert.fail(e.getMessage());
                    this.sqlgGraph.tx().commit();
                }
            });
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(6000, TimeUnit.SECONDS)) {
            Assert.fail("failed to terminate executor service normally");
        }
        Assert.assertEquals(2000, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabels().size());

    }
}
