package org.umlg.sqlg.sql.parse;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.umlg.sqlg.structure.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Date: 2015/01/03
 * Time: 1:06 PM
 */
public class GremlinParser<S extends Element, E extends Element> {

    private SqlgGraph sqlgGraph;
    private SchemaManager schemaManager;

    public GremlinParser(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.schemaManager = sqlgGraph.getSchemaManager();
    }


    /**
     * Constructs the label paths from the given schemaTable to the leaf vertex labels for the gremlin query.
     * For each path Sqlg will execute a sql query. The union of the queries is the result the gremlin query.
     * The vertex labels can be calculated from the steps.
     *
     * @param schemaTable
     * @param replacedSteps The original VertexSteps and HasSteps that were replaced
     * @return a List of paths. Each path is itself a list of SchemaTables.
     */
    public SchemaTableTree parse(SchemaTable schemaTable, List<ReplacedStep<S, E>> replacedSteps) {
        Set<SchemaTableTree> schemaTableTrees = new HashSet<>();
        SchemaTableTree rootSchemaTableTree = new SchemaTableTree(this.sqlgGraph, schemaTable, 0);
        schemaTableTrees.add(rootSchemaTableTree);
        for (ReplacedStep<S, E> replacedStep : replacedSteps) {
            //This schemaTableTree represents the tree nodes as build up to this depth. Each replacedStep goes a level further
            schemaTableTrees = replacedStep.calculatePathForStep(schemaTableTrees);
        }
        rootSchemaTableTree.removeAllButDeepestLeafNodes(replacedSteps.size());
        rootSchemaTableTree.removeNodesInvalidatedByHas();
        return rootSchemaTableTree;
    }




}
