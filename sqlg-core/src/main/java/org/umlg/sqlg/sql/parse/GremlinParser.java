package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Date: 2015/01/03
 * Time: 1:06 PM
 */
public class GremlinParser<S extends SqlgElement, E extends SqlgElement> {

    private SqlgGraph sqlgGraph;

    public GremlinParser(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
    }

    public Set<SchemaTableTree> parseForStrategy(List<ReplacedStep<S, E>> replacedSteps) {
        Set<SchemaTableTree> result = parse(replacedSteps);
        return result;
    }

    public Set<SchemaTableTree> parse(ReplacedStepTree replacedStepTree) {
        ReplacedStep startReplacedStep = replacedStepTree.root().getReplacedStep();
        Preconditions.checkState(startReplacedStep.isGraphStep(), "Step must be a GraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = startReplacedStep.getRootSchemaTableTrees(this.sqlgGraph, replacedStepTree.getDepth());
        Set<SchemaTableTree> toRemove = new HashSet<>();
        for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
            SqlgUtil.removeTopologyStrategyHasContainer(rootSchemaTableTree.getHasContainers());
            Set<SchemaTableTree> schemaTableTrees = new HashSet<>();
            schemaTableTrees.add(rootSchemaTableTree);
            replacedStepTree.walkReplacedSteps(schemaTableTrees);
            boolean remove = rootSchemaTableTree.removeNodesInvalidatedByHas();
            if (remove) {
                toRemove.add(rootSchemaTableTree);
            }
            rootSchemaTableTree.removeAllButDeepestAndAddCacheLeafNodes(replacedStepTree.getDepth());
        }
        rootSchemaTableTrees.removeAll(toRemove);
//        replacedSteps.add(0, startReplacedStep);
        return rootSchemaTableTrees;

    }

    /**
     * This is for the GraphStep
     * The first replacedStep existVertexLabel the starting SchemaTable.
     *
     * @param replacedSteps The list of steps that were replaced by the strategy.
     * @return The root {@link SchemaTableTree}s
     */
    public Set<SchemaTableTree> parse(List<ReplacedStep<S, E>> replacedSteps) {
        ReplacedStep startReplacedStep = replacedSteps.remove(0);
        startReplacedStep.cloneHasContainers();
        Preconditions.checkState(startReplacedStep.isGraphStep(), "Step must be a GraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = startReplacedStep.getRootSchemaTableTrees(this.sqlgGraph, replacedSteps.size());
        Set<SchemaTableTree> toRemove = new HashSet<>();
        for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
            SqlgUtil.removeTopologyStrategyHasContainer(rootSchemaTableTree.getHasContainers());
            Set<SchemaTableTree> schemaTableTrees = new HashSet<>();
            schemaTableTrees.add(rootSchemaTableTree);
            for (ReplacedStep<S, E> replacedStep : replacedSteps) {
                if (!(replacedStep.getStep() instanceof OrderGlobalStep) && !(replacedStep.getStep() instanceof RangeGlobalStep)) {
                    //This schemaTableTree represents the tree nodes as build up to this depth. Each replacedStep goes a level further
                    schemaTableTrees = replacedStep.calculatePathForStep(schemaTableTrees);
                }
            }
            boolean remove = rootSchemaTableTree.removeNodesInvalidatedByHas();
            if (remove) {
                toRemove.add(rootSchemaTableTree);
            }
            rootSchemaTableTree.removeAllButDeepestAndAddCacheLeafNodes(replacedSteps.size());
        }
        rootSchemaTableTrees.removeAll(toRemove);
        replacedSteps.add(0, startReplacedStep);
        startReplacedStep.resetHasContainers();
        return rootSchemaTableTrees;

    }

    /**
     * This is only called for local vertex steps.
     * Constructs the label paths from the given schemaTable to the leaf vertex labels for the gremlin query.
     * For each path Sqlg will executeRegularQuery a sql query. The union of the queries is the result the gremlin query.
     * The vertex labels can be calculated from the steps.
     *
     * @param schemaTable
     * @param replacedSteps The original VertexSteps and HasSteps that were replaced
     * @return a List of paths. Each path is itself a list of SchemaTables.
     */
    public SchemaTableTree parse(SchemaTable schemaTable, List<ReplacedStep<S, E>> replacedSteps) {
        Preconditions.checkArgument(!replacedSteps.get(0).isGraphStep(), "Expected VertexStep, found GraphStep");

        Set<SchemaTableTree> schemaTableTrees = new HashSet<>();
        //replacedSteps contains a fake label representing the incoming vertex for the SqlgVertexStepStrategy.
        SchemaTableTree rootSchemaTableTree = new SchemaTableTree(this.sqlgGraph, schemaTable, 0, replacedSteps.size() - 1);
        rootSchemaTableTree.setOptionalLeftJoin(replacedSteps.get(0).isLeftJoin());
        rootSchemaTableTree.setEmit(replacedSteps.get(0).isEmit());
        rootSchemaTableTree.setUntilFirst(replacedSteps.get(0).isUntilFirst());
        rootSchemaTableTree.initializeAliasColumnNameMaps();
        rootSchemaTableTree.setStepType(schemaTable.isVertexTable() ? SchemaTableTree.STEP_TYPE.VERTEX_STEP : SchemaTableTree.STEP_TYPE.EDGE_VERTEX_STEP);
        schemaTableTrees.add(rootSchemaTableTree);
        for (ReplacedStep<S, E> replacedStep : replacedSteps) {
            if (!replacedStep.isFake()) {
                //This schemaTableTree represents the tree nodes as build up to this depth. Each replacedStep goes a level further
                schemaTableTrees = replacedStep.calculatePathForStep(schemaTableTrees);
            }
        }
        rootSchemaTableTree.removeNodesInvalidatedByHas();
        rootSchemaTableTree.removeAllButDeepestAndAddCacheLeafNodes(replacedSteps.size() - 1);
        rootSchemaTableTree.setLocalStep(true);
        return rootSchemaTableTree;
    }

}
