package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.*;
import org.umlg.sqlg.test.batch.*;
import org.umlg.sqlg.test.branchstep.TestSqlgBranchStep;
import org.umlg.sqlg.test.complex.TestComplex;
import org.umlg.sqlg.test.complex.TestGithub;
import org.umlg.sqlg.test.datasource.TestCustomDataSource;
import org.umlg.sqlg.test.datasource.TestDataSource;
import org.umlg.sqlg.test.datasource.TestJNDIInitialization;
import org.umlg.sqlg.test.edgehas.TestEdgeHas;
import org.umlg.sqlg.test.edges.*;
import org.umlg.sqlg.test.event.TestTinkerPopEvent;
import org.umlg.sqlg.test.filter.and.TestAndStep;
import org.umlg.sqlg.test.filter.and.barrier.TestAndStepBarrier;
import org.umlg.sqlg.test.filter.connectivestep.TestAndandOrStep;
import org.umlg.sqlg.test.filter.not.barrier.TestNotStepBarrier;
import org.umlg.sqlg.test.filter.or.TestOrStep;
import org.umlg.sqlg.test.filter.or.TestOrStepAfterVertexStepBarrier;
import org.umlg.sqlg.test.filter.or.barrier.TestOrStepBarrier;
import org.umlg.sqlg.test.fold.TestFoldStep;
import org.umlg.sqlg.test.graph.MidTraversalGraphTest;
import org.umlg.sqlg.test.graph.TestEmptyGraph;
import org.umlg.sqlg.test.graph.TestGraphStepWithIds;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.idstep.TestIdStep;
import org.umlg.sqlg.test.index.TestIndex;
import org.umlg.sqlg.test.index.TestIndexOnPartition;
import org.umlg.sqlg.test.index.TestIndexTopologyTraversal;
import org.umlg.sqlg.test.inject.TestInject;
import org.umlg.sqlg.test.io.TestIo;
import org.umlg.sqlg.test.io.TestIoEdge;
import org.umlg.sqlg.test.json.TestJson;
import org.umlg.sqlg.test.json.TestJsonUpdate;
import org.umlg.sqlg.test.labels.TestHasLabelAndId;
import org.umlg.sqlg.test.labels.TestLabelLength;
import org.umlg.sqlg.test.labels.TestLabelsSchema;
import org.umlg.sqlg.test.labels.TestMultipleLabels;
import org.umlg.sqlg.test.localdate.TestLocalDate;
import org.umlg.sqlg.test.localdate.TestLocalDateArray;
import org.umlg.sqlg.test.localvertexstep.*;
import org.umlg.sqlg.test.match.TestMatch;
import org.umlg.sqlg.test.memory.TestMemoryUsage;
import org.umlg.sqlg.test.mod.*;
import org.umlg.sqlg.test.partition.TestJoinAcrossPartition;
import org.umlg.sqlg.test.process.dropstep.*;
import org.umlg.sqlg.test.properties.TestEscapedValues;
import org.umlg.sqlg.test.properties.TestPropertyValues;
import org.umlg.sqlg.test.reducing.*;
import org.umlg.sqlg.test.remove.TestRemoveEdge;
import org.umlg.sqlg.test.repeatstep.TestUnoptimizedRepeatStep;
import org.umlg.sqlg.test.roles.TestReadOnlyRole;
import org.umlg.sqlg.test.rollback.TestRollback;
import org.umlg.sqlg.test.sack.TestSack;
import org.umlg.sqlg.test.sample.TestSample;
import org.umlg.sqlg.test.schema.*;
import org.umlg.sqlg.test.topology.*;
import org.umlg.sqlg.test.travers.TestTraversals;
import org.umlg.sqlg.test.tree.TestColumnNamePropertyNameMapScope;
import org.umlg.sqlg.test.usersuppliedpk.topology.TestMultipleIDQuery;
import org.umlg.sqlg.test.usersuppliedpk.topology.TestSimpleJoinGremlin;
import org.umlg.sqlg.test.usersuppliedpk.topology.TestSimpleVertexEdgeGremlin;
import org.umlg.sqlg.test.usersuppliedpk.topology.TestUserSuppliedPKTopology;
import org.umlg.sqlg.test.uuid.TestUUID;
import org.umlg.sqlg.test.vertex.*;
import org.umlg.sqlg.test.vertexout.TestVertexOutWithHas;
import org.umlg.sqlg.test.where.TestTraversalFilterStepBarrier;

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
        TestHasLabelAndId.class,
        TestLoadArrayProperties.class,
        TestLoadElementProperties.class,
        TestLoadSchema.class,
        TestPool.class,
        TestRemoveElement.class,
        TestSetProperty.class,
        TestVertexCreation.class,
        TestVertexEdgeSameName.class,
        TestVertexNavToEdges.class,
        // are these tests gone?
        //TestByteArray.class,
        //TestQuery.class,
        TestSchema.class,
        TestIndex.class,
        TestVertexOutWithHas.class,
        TestEdgeHas.class,
        TestBatch.class,
        TestBatchNormalUpdate.class,
        TestMultiThreadedBatch.class,
        TestMultiThread.class,
        TestMultipleThreadMultipleJvm.class,

        TestRemoveEdge.class,
        TestEdgeSchemaCreation.class,
        TestRollback.class,

        TestNewVertex.class,
        TestEdgeCache.class,
        TestTinkerpopBug.class,
        TestLoadSchemaViaNotify.class,
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
        TestGremlinCompileGraphV.class,
        TestGremlinCompileWhere.class,
        TestGremlinCompileTextPredicate.class,
        TestGremlinCompileFullTextPredicate.class,
        TestGremlinCompileChoose.class,
        TestGremlinCompileVertexStep.class,
        TestGremlinCompileWhereLocalDate.class,
        TestGremlinCompileArrayContains.class,
        TestGremlinCompileArrayOverlaps.class,
        TestColumnNameTranslation.class,
        TestGraphStepOrderBy.class,
        TestVertexStepOrderBy.class,
        TestPathStep.class,
        TestLocalDate.class,
        TestLocalDateArray.class,
        TestBatchStreamVertex.class,
        TestBatchStreamEdge.class,
        TestJson.class,
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
        TestRepeatStepWithLabels.class,
        TestGraphStepWithIds.class,
        TestOtherVertex.class,
        TestGremlinMod.class,
        TestTopologyUpgrade.class,
        TestTopologyMultipleGraphs.class,
        TestTraversals.class,
        TestGremlinOptional.class,
        TestAlias.class,
        TestGithub.class,
        TestLocalVertexStepOptional.class,
        TestLocalVertexStepRepeatStep.class,
        TestLocalEdgeVertexStep.class,
        TestLocalEdgeOtherVertexStep.class,
        TestBatchNormalDateTime.class,
        TestBatchEdgeDateTime.class,
        TestBatchJson.class,
        TestMemoryUsage.class,
        TestBatchStreamTemporaryVertex.class,
        TestBatchNormalPrimitiveArrays.class,
        TestBatchNormalPrimitive.class,
        TestBatchNormalUpdatePrimitiveArrays.class,
        TestJsonUpdate.class,
        TestBatchNormalUpdateDateTime.class,
        TestOptionalWithOrder.class,
        TestMultipleLabels.class,
        TestColumnNamePropertyNameMapScope.class,
        TestSchemaTableTreeAndHasContainer.class,
        TestEscapedValues.class,
        TestRepeatStepOnEdges.class,
        TestLoadingAdjacent.class,
        TestLabelsSchema.class,
        MidTraversalGraphTest.class,
        //TODO fails, issue #65
//        TestEdgeFromDifferentSchema.class
        TestBatchModeMultipleGraphs.class,
        TestDetachedEdge.class,
        TestSchemaEagerCreation.class,
        TestIndexTopologyTraversal.class,
        TestNotifyJson.class,
        TestVertexEdges.class,
        TestSqlgSchema.class,
        TestValidateTopology.class,
        TestBatchNormalUpdateDateTimeArrays.class,
        TestTopologyChangeListener.class,
        TestRangeLimit.class,
        TestReplacedStepEmitComparator.class,
        TestLocalStepCompile.class,
        TestLocalVertexStepLimit.class,
        TestLocalVertexStepOptionalWithOrder.class,
        TestOptionalWithRange.class,
        TestRepeatWithOrderAndRange.class,
        TestMatch.class,
        TestSqlgBranchStep.class,
        TestLocalVertexStepWithOrder.class,
        TestMax.class,
        TestGroupCount.class,
        TestSack.class,
        TestSample.class,
        TestTopologyChangeListener.class,
        TestTopologyDelete.class,
        TestTopologyDeleteSpecific.class,
        TestTopologyDeleteEdgeRole.class,
        TestTinkerPopEvent.class,
        TestIo.class,
        TestComplex.class,
        TestTopologyDeleteSpecific.class,
        TestDeadLock.class,
        TestLabelLength.class,
        TestAddTemporaryVertex.class,
        TestIoEdge.class,
        TestBatchTemporaryVertex.class,
        TestUnoptimizedRepeatStep.class,
        TestTraversalFilterStepBarrier.class,
        TestOrStepBarrier.class,
        TestAndStepBarrier.class,
        TestNotStepBarrier.class,
        TestOrStep.class,
        TestAndStep.class,
        TestAndandOrStep.class,
        TestOrStepAfterVertexStepBarrier.class,
        TestDropStep.class,
        TestDropStepBarrier.class,
        TestDropStepTruncate.class,
        TestTopologyGraph.class,
        TestUnoptimizedRepeatStep.class,
        TestPropertyReference.class,
        TestPartitioning.class,
        TestJoinAcrossPartition.class,
        TestPartitionMultipleGraphs.class,
        TestSubSubPartition.class,
        TestIndexOnPartition.class,
        TestUserSuppliedPKTopology.class,
        TestSimpleJoinGremlin.class,
        TestSimpleVertexEdgeGremlin.class,
        TestMultipleIDQuery.class,
//        TestSharding.class,
//        TestShardingGremlin.class,
        TestRecordId.class,
        TestPropertyValues.class,
        TestReadOnlyRole.class,
        TestVarChar.class,
        TestTopologySchemaDeleteMultipleGraphs.class,
        TestTraversalAddV.class,
        TestDataSource.class,
        TestCustomDataSource.class,
        TestJNDIInitialization.class,
        TestReducing.class,
        TestReducingVertexStep.class,
        TestPartitionedDrop.class,
        TestPartitionedDrop.class,
        TestDropStepPartition.class,
        TestBatchUpdatePartitioning.class,
        TestLargeSchemaPerformance.class,
        TestInject.class,
        TestFoldStep.class,
        TestForeignSchema.class,
        TestTopologyPropertyColumnRename.class,
        TestTopologyPropertyColumnRenameDistributed.class,
        TestTopologyVertexLabelRename.class,
        TestTopologyVertexLabelRenameDistributed.class,
        TestTopologyEdgeLabelRename.class,
        TestTopologyVertexLabelWithIdentifiersRenameDistributed.class,
        TestTopologyVertexLabelWithIdentifiersRename.class,
        TestTopologyEdgeLabelWithIdentifiersRename.class,
        TestTopologyEdgeLabelRenameDistributed.class,
        TestUUID.class,
        TestIdStep.class,
        TestTopologyLock.class,
        TestLoadEdgeWithSpecialCharacters.class

})
public class AllTest {

}
