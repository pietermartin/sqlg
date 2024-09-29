package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.strategy.SqlgComparatorHolder;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Date: 2015/01/03
 * Time: 1:06 PM
 */
public class GremlinParser {

    private final SqlgGraph sqlgGraph;

    public GremlinParser(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
    }

    public Set<SchemaTableTree> parse(ReplacedStepTree<?, ?> replacedStepTree) {
        ReplacedStep<?, ?> startReplacedStep = replacedStepTree.root().getReplacedStep();
        Preconditions.checkState(startReplacedStep.isGraphStep(), "Step must be a GraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = startReplacedStep.calculateRootSchemaTableTrees(this.sqlgGraph, replacedStepTree.getDepth());

        if (startReplacedStep.getRecursiveRepeatStepConfig() != null) {
            //We create a separate SchemaTableTree for the until traversal.
            //This is needed to generate the WHERE clause
            Preconditions.checkState(rootSchemaTableTrees.size() == 1);
            SchemaTableTree rootSchemaTableTree = rootSchemaTableTrees.iterator().next();
            Preconditions.checkState(startReplacedStep.getRecursiveRepeatStepConfig() == rootSchemaTableTree.getRecursiveRepeatStepConfig());
            ReplacedStepTree<Vertex, Vertex> untilReplacedStepTree = startReplacedStep.getRecursiveRepeatStepConfig().untilReplacedStepTree();

            ReplacedStep<Vertex, Vertex> untilReplacedStep = untilReplacedStepTree.root().getReplacedStep();
            Set<SchemaTableTree> untilTraversalRootSchemaTableTrees = untilReplacedStep.calculateRootSchemaTableTrees(this.sqlgGraph, untilReplacedStepTree.getDepth());

            Preconditions.checkState(untilTraversalRootSchemaTableTrees.size() == 1);
            SchemaTableTree untilSchemaTableTree = untilTraversalRootSchemaTableTrees.iterator().next();
            untilReplacedStepTree.walkReplacedSteps(untilSchemaTableTree);
            untilSchemaTableTree.removeAllButDeepestAndAddCacheLeafNodes(untilReplacedStepTree.getDepth());
            rootSchemaTableTree.setUntilTraversalRootSchemaTableTree(untilSchemaTableTree);
        }

        Set<SchemaTableTree> result = new HashSet<>();
        for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
            replacedStepTree.walkReplacedSteps(rootSchemaTableTree);
            boolean remove = rootSchemaTableTree.removeNodesInvalidatedByHas();
            remove = rootSchemaTableTree.removeNodesInvalidatedByRestrictedProperties() || remove;
            rootSchemaTableTree.removeAllButDeepestAndAddCacheLeafNodes(replacedStepTree.getDepth());
            if (!remove && rootSchemaTableTree.hasLeafNodes()) {
                rootSchemaTableTree.close();
                result.add(rootSchemaTableTree);
            }
        }
        return result;
    }


    /**
     * This is only called for vertex steps.
     * Constructs the label paths from the given schemaTable to the leaf vertex labels for the gremlin query.
     * For each path Sqlg will executeRegularQuery a sql query. The union of the queries is the result the gremlin query.
     * The vertex labels can be calculated from the steps.
     *
     * @param schemaTable      The schema and table
     * @param replacedStepTree The original VertexSteps and HasSteps that were replaced
     * @return a List of paths. Each path is itself a list of SchemaTables.
     */
    public SchemaTableTree parse(SchemaTable schemaTable, ReplacedStepTree<?, ?> replacedStepTree, boolean isSqlgLocalStepBarrierChild) {
        ReplacedStep<?, ?> rootReplacedStep = replacedStepTree.root().getReplacedStep();
        Preconditions.checkArgument(!rootReplacedStep.isGraphStep(), "Expected VertexStep, found GraphStep");

        //replacedSteps contains a fake label representing the incoming vertex for the SqlgVertexStepStrategy.
        SchemaTableTree rootSchemaTableTree = new SchemaTableTree(
                this.sqlgGraph,
                null,
                schemaTable,
                null,
                schemaTable.isVertexTable() ? SchemaTableTree.STEP_TYPE.VERTEX_STEP : SchemaTableTree.STEP_TYPE.EDGE_VERTEX_STEP,
                0,
                replacedStepTree.getDepth(),
                rootReplacedStep.isIdOnly(),
                new ArrayList<>(),
                new ArrayList<>(),
                new SqlgComparatorHolder(),
                new ArrayList<>(),
                rootReplacedStep.isUntilFirst(),
                rootReplacedStep.isEmit(),
                rootReplacedStep.isLeftJoin(),
                false,
                false,
                true,
                isSqlgLocalStepBarrierChild,
                null,
                rootReplacedStep.getGroupBy(),
                rootReplacedStep.getAggregateFunction()
        );

        rootSchemaTableTree.initializeAliasColumnNameMaps();
        rootSchemaTableTree.getRestrictedProperties().addAll(rootReplacedStep.getRestrictedProperties());
        replacedStepTree.walkReplacedSteps(rootSchemaTableTree);
        rootSchemaTableTree.removeNodesInvalidatedByHas();
        rootSchemaTableTree.removeAllButDeepestAndAddCacheLeafNodes(replacedStepTree.getDepth());

        return rootSchemaTableTree;
    }

}
