package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;

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

    public Set<SchemaTableTree> parse(ReplacedStepTree replacedStepTree) {
        ReplacedStep<?, ?> startReplacedStep = replacedStepTree.root().getReplacedStep();
        Preconditions.checkState(startReplacedStep.isGraphStep(), "Step must be a GraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = startReplacedStep.getRootSchemaTableTrees(this.sqlgGraph, replacedStepTree.getDepth());
        Set<SchemaTableTree> toRemove = new HashSet<>();
        for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
            Set<SchemaTableTree> schemaTableTrees = new HashSet<>();
            schemaTableTrees.add(rootSchemaTableTree);
            replacedStepTree.walkReplacedSteps(schemaTableTrees);
            boolean remove = rootSchemaTableTree.removeNodesInvalidatedByHas();
            if (remove) {
                toRemove.add(rootSchemaTableTree);
            }
            remove = rootSchemaTableTree.removeNodesInvalidatedByRestrictedProperties();
            if (remove) {
                toRemove.add(rootSchemaTableTree);
            }
            rootSchemaTableTree.removeAllButDeepestAndAddCacheLeafNodes(replacedStepTree.getDepth());
            if (!rootSchemaTableTree.hasLeafNodes()) {
                toRemove.add(rootSchemaTableTree);
            }
        }
        rootSchemaTableTrees.removeAll(toRemove);
        return rootSchemaTableTrees;

    }


    /**
     * This is only called for vertex steps.
     * Constructs the label paths from the given schemaTable to the leaf vertex labels for the gremlin query.
     * For each path Sqlg will executeRegularQuery a sql query. The union of the queries is the result the gremlin query.
     * The vertex labels can be calculated from the steps.
     *
     * @param schemaTable The schema and table
     * @param replacedStepTree The original VertexSteps and HasSteps that were replaced
     * @return a List of paths. Each path is itself a list of SchemaTables.
     */
    public SchemaTableTree parse(SchemaTable schemaTable, ReplacedStepTree replacedStepTree, boolean isSqlgLocalStepBarrierChild) {
        ReplacedStep<?, ?> rootReplacedStep = replacedStepTree.root().getReplacedStep();
        Preconditions.checkArgument(!rootReplacedStep.isGraphStep(), "Expected VertexStep, found GraphStep");

        Set<SchemaTableTree> schemaTableTrees = new HashSet<>();
        //replacedSteps contains a fake label representing the incoming vertex for the SqlgVertexStepStrategy.
        SchemaTableTree rootSchemaTableTree = new SchemaTableTree(this.sqlgGraph, schemaTable, 0, replacedStepTree.getDepth());
        rootSchemaTableTree.setOptionalLeftJoin(rootReplacedStep.isLeftJoin());
        rootSchemaTableTree.setEmit(rootReplacedStep.isEmit());
        rootSchemaTableTree.setUntilFirst(rootReplacedStep.isUntilFirst());
        rootSchemaTableTree.initializeAliasColumnNameMaps();

        rootSchemaTableTree.setRestrictedProperties(rootReplacedStep.getRestrictedProperties());
        rootSchemaTableTree.setAggregateFunction(rootReplacedStep.getAggregateFunction());
        rootSchemaTableTree.setGroupBy(rootReplacedStep.getGroupBy());

        rootSchemaTableTree.setStepType(schemaTable.isVertexTable() ? SchemaTableTree.STEP_TYPE.VERTEX_STEP : SchemaTableTree.STEP_TYPE.EDGE_VERTEX_STEP);
        schemaTableTrees.add(rootSchemaTableTree);
        replacedStepTree.walkReplacedSteps(schemaTableTrees);
        rootSchemaTableTree.removeNodesInvalidatedByHas();
        rootSchemaTableTree.removeAllButDeepestAndAddCacheLeafNodes(replacedStepTree.getDepth());
        rootSchemaTableTree.setLocalStep(true);
        rootSchemaTableTree.setLocalBarrierStep(isSqlgLocalStepBarrierChild);
        return rootSchemaTableTree;
    }

}
