package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.*;
import org.umlg.sqlg.test.batch.*;
import org.umlg.sqlg.test.edgehas.TestEdgeHas;
import org.umlg.sqlg.test.edges.*;
import org.umlg.sqlg.test.github.TestGithub;
import org.umlg.sqlg.test.graph.TestEmptyGraph;
import org.umlg.sqlg.test.graph.TestGraphStepWithIds;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.index.TestForeignKeyIndexPerformance;
import org.umlg.sqlg.test.index.TestIndex;
import org.umlg.sqlg.test.json.JsonTest;
import org.umlg.sqlg.test.localdate.LocalDateTest;
import org.umlg.sqlg.test.mod.*;
import org.umlg.sqlg.test.multithread.TestMultiThread;
import org.umlg.sqlg.test.remove.TestRemoveEdge;
import org.umlg.sqlg.test.rollback.TestRollback;
import org.umlg.sqlg.test.schema.*;
import org.umlg.sqlg.test.travers.TestTraversals;
import org.umlg.sqlg.test.upgrade.TestTopologyUpgrade;
import org.umlg.sqlg.test.vertex.*;
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
        TestBatchUpdate.class,
        TestForeignKeyIndexPerformance.class,
        TestMultiThreadedBatch.class,
        TestRemoveEdge.class,
        TestEdgeSchemaCreation.class,
        TestRollback.class,
        TestMultiThread.class,
        TestNewVertex.class,
        TestEdgeCache.class,
        TestVertexCache.class,
        TestTinkerpopBug.class,
        TestLazyLoadSchema.class,
        TestCreateEdgeBetweenVertices.class,
        TestRemovedVertex.class,
        TestCaptureSchemaTableEdges.class,
        TestGremlinCompileWithHas.class,
        TestGremlinCompileE.class,
        TestEmptyGraph.class,
        TestOutE.class,
        TestForeignKeysAreOptional.class,
        TestGremlinCompileWithAs.class,
        TestGremlinCompileWithInOutV.class,
        TestGremlinCompileV.class,
        TestGremlinCompileGraphStep.class,
        TestGremlinCompileWhere.class,
        TestColumnNameTranslation.class,
        TestGraphStepOrderBy.class,
        TestAggregate.class,
        TestVertexStepOrderBy.class,
        TestPathStep.class,
        LocalDateTest.class,
        TestStreamVertex.class,
        TestStreamingEdge.class,
        JsonTest.class,
        TestSchemaManagerGetTablesFor.class,
        TestBatchServerSideEdgeCreation.class,
        TestBatchedStreaming.class,
        TestBulkWithin.class,
        TestBulkWithout.class,
        TestRemoveProperty.class,
        TestSchemaManagerGetTablesFor.class,
        TestAggregate.class,
        TestTreeStep.class,
        TestRepeatStepGraphOut.class,
        TestRepeatStepGraphIn.class,
        TestRepeatStepVertexOut.class,
        TestRepeatStepGraphBoth.class,
        TestGraphStepWithIds.class,
        TestOtherVertex.class,
        TestGremlinMod.class,
        TestBatchEdge.class,
        TestTopologyUpgrade.class,
        TestTraversals.class,
        TestGremlinOptional.class
        TestAlias.class,
        TestGithub.class
})
public class AllTest {
}
