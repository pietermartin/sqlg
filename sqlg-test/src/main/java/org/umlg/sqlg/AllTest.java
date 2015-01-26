package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.*;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestMultiThreadedBatch;
import org.umlg.sqlg.test.edgehas.TestEdgeHas;
import org.umlg.sqlg.test.edges.TestCreateEdgeBetweenVertices;
import org.umlg.sqlg.test.edges.TestEdgeCache;
import org.umlg.sqlg.test.edges.TestEdgeSchemaCreation;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileE;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileV;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithHas;
import org.umlg.sqlg.test.index.TestForeignKeyIndexPerformance;
import org.umlg.sqlg.test.index.TestIndex;
import org.umlg.sqlg.test.mod.*;
import org.umlg.sqlg.test.multithread.TestMultiThread;
import org.umlg.sqlg.test.remove.TestRemoveEdge;
import org.umlg.sqlg.test.rollback.TestRollback;
import org.umlg.sqlg.test.schema.TestCaptureSchemaTableEdges;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.schema.TestSchema;
import org.umlg.sqlg.test.vertex.TestNewVertex;
import org.umlg.sqlg.test.vertex.TestRemovedVertex;
import org.umlg.sqlg.test.vertex.TestTinkerpopBug;
import org.umlg.sqlg.test.vertex.TestVertexCache;
import org.umlg.sqlg.test.vertexout.TestVertexOutWithHas;

/**
 * Date: 2014/07/16
 * Time: 12:08 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestAddVertexViaMap.class,
        TestAllEdges.class,
        TestAllVertices.class,
        TestArrayProperties.class,
        TestCountVerticesAndEdges.class,
        TestDeletedVertex.class,
        TestEdgeCreation.class,
        TestEdgeToDifferentLabeledVertexes.class,
        TestGetById.class,
        TestHas.class,
        TestHasLabel.class,
        TestLoadArrayProperties.class,
        TestLoadElementProperties.class,
        TestLoadSchema.class,
        TestPool.class,
        TestRemoveElement.class,
        TestRemoveProperty.class,
        TestSetProperty.class,
        TestVertexCreation.class,
        TestVertexEdgeSameName.class,
        TestVertexNavToEdges.class,
        TestByteArray.class,
        TestQuery.class,
        TestSchema.class,
        TestIndex.class,
        TestVertexOutWithHas.class,
        TestEdgeHas.class,
        TestBatch.class,
        TestRemoveEdge.class,
        TestEdgeSchemaCreation.class,
        TestRollback.class,
        TestMultiThread.class,
        TestForeignKeyIndexPerformance.class,
        TestNewVertex.class,
        TestEdgeCache.class,
        TestVertexCache.class,
        TestTinkerpopBug.class,
//        TestLazyLoadSchema.class,
        TestMultiThreadedBatch.class,
        TestCreateEdgeBetweenVertices.class,
        TestRemovedVertex.class,
        TestCaptureSchemaTableEdges.class,
        TestGremlinCompileWithHas.class,
        TestGremlinCompileV.class,
        TestGremlinCompileE.class
})
public class AllTest {
}
