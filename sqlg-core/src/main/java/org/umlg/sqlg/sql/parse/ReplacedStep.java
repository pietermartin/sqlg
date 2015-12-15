package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.umlg.sqlg.strategy.BaseSqlgStrategy;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Date: 2015/06/27
 * Time: 6:05 PM
 */
public class ReplacedStep<S, E> {

    private SchemaManager schemaManager;
    private AbstractStep<S, E> step;
    private Set<String> labels;
    private List<HasContainer> hasContainers;
    private List<Comparator> comparators;
    //This indicates the distanced of the replaced steps from the starting step. i.e. g.V(1).out().out().out() will be 0,1,2 for the 3 outs
    private int depth;
    private boolean emit;
    private boolean untilFirst;
    private boolean emitFirst;
    //This is indicate whether a where clause is needed in the sql
    private boolean isVertexGraphStep = false;

    public static <S, E> ReplacedStep from(SchemaManager schemaManager, AbstractStep<S, E> step, int pathCount) {
        ReplacedStep replacedStep = new ReplacedStep<>();
        replacedStep.step = step;
        replacedStep.labels = step.getLabels().stream().map(l -> pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + l).collect(Collectors.toSet());
        replacedStep.hasContainers = new ArrayList<>();
        replacedStep.comparators = new ArrayList<>();
        replacedStep.schemaManager = schemaManager;
        return replacedStep;
    }

    public void setVertexGraphStep(boolean vertexGraphStep) {
        isVertexGraphStep = vertexGraphStep;
    }

    public List<HasContainer> getHasContainers() {
        return hasContainers;
    }

    public List<Comparator> getComparators() {
        return comparators;
    }

    public void addLabel(String label) {
        this.labels.add(label);
    }

    public Set<String> getLabels() {
        return this.labels;
    }

    public Set<SchemaTableTree> appendPath(SchemaTableTree schemaTableTree) {
        if (this.step instanceof VertexStep) {
            return appendPathForVertexStep(schemaTableTree);
        } else if (this.step instanceof EdgeVertexStep) {
            return appendPathForEdgeVertexStep(schemaTableTree);
        } else if (this.step instanceof EdgeOtherVertexStep) {
            return appendPathForEdgeOtherVertexStep(schemaTableTree);
        } else {
            throw new IllegalStateException("Only VertexStep, EdgeVertexStep and EdgeOtherVertexStep are handled! Found " + this.step.getClass().getName());
        }
    }

    private Set<SchemaTableTree> appendPathForEdgeVertexStep(SchemaTableTree schemaTableTree) {
        EdgeVertexStep edgeVertexStep = (EdgeVertexStep) this.step;
        return calculatePathFromEdgeToVertex(schemaTableTree, schemaTableTree.getSchemaTable(), edgeVertexStep.getDirection());
    }

    private Set<SchemaTableTree> appendPathForEdgeOtherVertexStep(SchemaTableTree schemaTableTree) {
        Preconditions.checkArgument(schemaTableTree.getDirection() != Direction.BOTH, "ReplacedStep.appendPathForEdgeOtherVertexStep schemaTableTree may not have direction BOTH");
        EdgeOtherVertexStep edgeOtherVertexStep = (EdgeOtherVertexStep) this.step;
        return calculatePathFromEdgeToVertex(schemaTableTree, schemaTableTree.getSchemaTable(), (schemaTableTree.getDirection() == Direction.IN ? Direction.OUT : Direction.IN));
    }

    private Set<SchemaTableTree> appendPathForVertexStep(SchemaTableTree schemaTableTree) {
        Preconditions.checkArgument(schemaTableTree.getSchemaTable().isVertexTable(), "Expected a Vertex table found " + schemaTableTree.getSchemaTable().getTable());

        Set<SchemaTableTree> result = new HashSet<>();
        Pair<Set<SchemaTable>, Set<SchemaTable>> inAndOutLabelsFromCurrentPosition = this.schemaManager.getTableLabels(schemaTableTree.getSchemaTable());

        VertexStep vertexStep = (VertexStep) this.step;
        String[] edgeLabels = vertexStep.getEdgeLabels();
        Direction direction = vertexStep.getDirection();
        Class<? extends Element> elementClass = vertexStep.getReturnClass();

        Set<SchemaTable> inLabels = inAndOutLabelsFromCurrentPosition != null ? inAndOutLabelsFromCurrentPosition.getLeft() : new HashSet<>();
        Set<SchemaTable> outLabels = inAndOutLabelsFromCurrentPosition != null ? inAndOutLabelsFromCurrentPosition.getRight() : new HashSet<>();
        Set<SchemaTable> inLabelsToTraversers;
        Set<SchemaTable> outLabelsToTraversers;
        switch (vertexStep.getDirection()) {
            case IN:
                inLabelsToTraversers = filter(inLabels, edgeLabels);
                outLabelsToTraversers = new HashSet<>();
                break;
            case OUT:
                outLabelsToTraversers = filter(outLabels, edgeLabels);
                inLabelsToTraversers = new HashSet<>();
                break;
            case BOTH:
                inLabelsToTraversers = edgeLabels.length > 0 ? filter(inLabels, edgeLabels) : inLabels;
                outLabelsToTraversers = edgeLabels.length > 0 ? filter(outLabels, edgeLabels) : outLabels;
                break;
            default:
                throw new IllegalStateException("Unknown direction " + direction.name());
        }

        if (elementClass.isAssignableFrom(Edge.class)) {
            inLabelsToTraversers = filterEdgeOnIdHasContainers(inLabelsToTraversers);
            outLabelsToTraversers = filterEdgeOnIdHasContainers(outLabelsToTraversers);
        }

        //Each labelToTravers more than the first one forms a new distinct path
        for (SchemaTable inLabelsToTravers : inLabelsToTraversers) {
            if (elementClass.isAssignableFrom(Edge.class)) {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                        inLabelsToTravers, Direction.IN, elementClass,
                        this,
                        this.labels);
                result.add(schemaTableTreeChild);
            } else {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                        inLabelsToTravers,
                        Direction.IN,
                        elementClass,
                        this,
                        Collections.EMPTY_SET);
                result.addAll(calculatePathFromVertexToEdge(schemaTableTreeChild, inLabelsToTravers, Direction.IN));
            }
        }
        //if emit and no where to traverse to add in a dummy.
        //this is required for the SqlgGraphStepCompiled to know that their is no last element to emit
        if (inLabelsToTraversers.isEmpty() && (vertexStep.getDirection() == Direction.BOTH || vertexStep.getDirection() == Direction.IN)) {
            schemaTableTree.leafNodeIsEmpty();
        }
        for (SchemaTable outLabelsToTravers : outLabelsToTraversers) {
            if (elementClass.isAssignableFrom(Edge.class)) {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                        outLabelsToTravers, Direction.OUT, elementClass,
                        this,
                        this.labels);
                result.add(schemaTableTreeChild);
            } else {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                        outLabelsToTravers, Direction.OUT, elementClass,
                        this,
                        Collections.EMPTY_SET);
                result.addAll(calculatePathFromVertexToEdge(schemaTableTreeChild, outLabelsToTravers, Direction.OUT));
            }
        }
        //if emit and no where to traverse to add in a dummy.
        //this is required for the SqlgGraphStepCompiled to know that their is no last element to emit
        if (outLabelsToTraversers.isEmpty() && (vertexStep.getDirection() == Direction.BOTH || vertexStep.getDirection() == Direction.OUT)) {
            schemaTableTree.leafNodeIsEmpty();
        }
        return result;
    }

    private Set<SchemaTable> filterEdgeOnIdHasContainers(Set<SchemaTable> inLabelsToTraversers) {
        Set<SchemaTable> idFilteredResult = new HashSet<>(inLabelsToTraversers);
        //Filter out labels if their is a hasContainer on the id field
        for (HasContainer idHasContainer : getIdHasContainer()) {
            SchemaTable hasContainerSchemaTable = RecordId.from(idHasContainer.getValue().toString()).getSchemaTable();
            hasContainerSchemaTable = SchemaTable.of(hasContainerSchemaTable.getSchema(), SchemaManager.EDGE_PREFIX + hasContainerSchemaTable.getTable());
            for (SchemaTable schemaTable : inLabelsToTraversers) {
                if (!schemaTable.equals(hasContainerSchemaTable)) {
                    idFilteredResult.remove(schemaTable);
                }
            }
        }
        return idFilteredResult;
    }

    private Set<SchemaTableTree> filterVertexOnIdHasContainers(Set<SchemaTableTree> inLabelsToTraversers) {
        Set<SchemaTableTree> idFilteredResult = new HashSet<>(inLabelsToTraversers);
        //Filter out labels if there is a hasContainer on the id field
        for (HasContainer idHasContainer : getIdHasContainer()) {
            SchemaTable hasContainerSchemaTable = RecordId.from(idHasContainer.getValue().toString()).getSchemaTable();
            hasContainerSchemaTable = SchemaTable.of(hasContainerSchemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + hasContainerSchemaTable.getTable());
            for (SchemaTableTree schemaTableTree : inLabelsToTraversers) {
                if (!schemaTableTree.getSchemaTable().equals(hasContainerSchemaTable)) {
                    idFilteredResult.remove(schemaTableTree);
                }
            }
        }
        return idFilteredResult;
    }

    private Set<SchemaTableTree> calculatePathFromEdgeToVertex(SchemaTableTree schemaTableTree, SchemaTable labelToTravers, Direction direction) {
        Preconditions.checkArgument(labelToTravers.isEdgeTable());
        Set<SchemaTableTree> result = new HashSet<>();
        Map<String, Set<String>> edgeForeignKeys = this.schemaManager.getAllEdgeForeignKeys();
        //join from the edge table to the incoming vertex table
        Set<String> foreignKeys = edgeForeignKeys.get(labelToTravers.toString());
        //Every foreignKey for the given direction must be joined on
        for (String foreignKey : foreignKeys) {
            String[] split = foreignKey.split("\\.");
            String foreignKeySchema = split[0];
            String foreignKeyTable = split[1];
            if ((direction == Direction.BOTH || direction == Direction.OUT) && foreignKey.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                        SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + SqlgUtil.removeTrailingOutId(foreignKeyTable)),
                        Direction.OUT,
                        Vertex.class,
                        this,
                        true,
                        this.labels
                );
                result.add(schemaTableTreeChild);
            }
            if ((direction == Direction.BOTH || direction == Direction.IN) && foreignKey.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                        SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + SqlgUtil.removeTrailingInId(foreignKeyTable)),
                        Direction.IN,
                        Vertex.class,
                        this,
                        true,
                        this.labels
                );
                result.add(schemaTableTreeChild);
            }
        }
        return result;
    }

    private Set<SchemaTableTree> calculatePathFromVertexToEdge(SchemaTableTree schemaTableTree, SchemaTable labelToTravers, Direction direction) {
        Preconditions.checkArgument(!labelToTravers.isVertexTable());
        Set<SchemaTableTree> result = new HashSet<>();
        Map<String, Set<String>> edgeForeignKeys = this.schemaManager.getAllEdgeForeignKeys();
        //join from the edge table to the incoming vertex table
        Set<String> foreignKeys = edgeForeignKeys.get(labelToTravers.toString());
        //Every foreignKey for the given direction must be joined on
        for (String foreignKey : foreignKeys) {
            String[] split = foreignKey.split("\\.");
            String foreignKeySchema = split[0];
            String foreignKeyTable = split[1];
            if (direction == Direction.IN && foreignKey.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                SchemaTableTree schemaTableTree1 = schemaTableTree.addChild(
                        SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + SqlgUtil.removeTrailingOutId(foreignKeyTable)),
                        direction,
                        Vertex.class,
                        this,
                        this.labels
                );
                result.add(schemaTableTree1);
            } else if (direction == Direction.OUT && foreignKey.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                SchemaTableTree schemaTableTree1 = schemaTableTree.addChild(
                        SchemaTable.of(foreignKeySchema, SchemaManager.VERTEX_PREFIX + SqlgUtil.removeTrailingInId(foreignKeyTable)),
                        direction,
                        Vertex.class,
                        this,
                        this.labels
                );
                result.add(schemaTableTree1);
            }
        }
        return filterVertexOnIdHasContainers(result);
    }

    public Set<SchemaTableTree> calculatePathForStep(Set<SchemaTableTree> schemaTableTrees) {
        Set<SchemaTableTree> result = new HashSet<>();
        for (SchemaTableTree schemaTableTree : schemaTableTrees) {
            result.addAll(this.appendPath(schemaTableTree));
        }
        return result;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    private Set<SchemaTable> filter(Set<SchemaTable> labels, String[] edgeLabels) {
        Set<SchemaTable> result = new HashSet<>();
        List<String> edges = Arrays.asList(edgeLabels);
        for (SchemaTable label : labels) {
            if (!label.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
                throw new IllegalStateException("Expected label to start with " + SchemaManager.EDGE_PREFIX);
            }
            String rawLabel = label.getTable().substring(SchemaManager.EDGE_PREFIX.length());
            //only filter if there are edges to filter
            if (!edges.isEmpty()) {
                if (edges.contains(rawLabel)) {
                    result.add(label);
                }
            } else {
                result.add(label);
            }
        }
        return result;
    }

    private List<HasContainer> getIdHasContainer() {
        return this.hasContainers.stream().filter(h -> h.getKey().equals(T.id.getAccessor())).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return this.step.toString() + " :: " + this.hasContainers.toString();
    }

    public boolean isGraphStep() {
        return this.step instanceof GraphStep;
    }

    public boolean isVertexStep() {
        return this.step instanceof VertexStep;
    }

    public boolean isEdgeVertexStep() {
        return this.step instanceof EdgeVertexStep;
    }

    /**
     * Calculates the root labels from which to start the query construction.
     *
     * @param sqlgGraph
     * @return A set of SchemaTableTree. A SchemaTableTree for each root label.
     */
    Set<SchemaTableTree> getRootSchemaTableTrees(SqlgGraph sqlgGraph) {
        Preconditions.checkState(this.isGraphStep(), "ReplacedStep must be for a GraphStep!");
        GraphStep graphStep = (GraphStep) this.step;

        //This list is reset the hasContainer back to its original state afterwards
        List<HasContainer> toRemove = new ArrayList<>();

        //If the graph step has ids then add in HasContainers for every distinct RecordId's SchemaTable
        Map<SchemaTable, List<Long>> groupedIds = null;
        if (graphStep.getIds().length > 0) {

            groupedIds = groupIdsBySchemaTable(graphStep);

            for (Map.Entry<SchemaTable, List<Long>> schemaTableSetEntry : groupedIds.entrySet()) {
                SchemaTable schemaTable = schemaTableSetEntry.getKey();

                //if the label is already in the hasContainers do not add it again.
                boolean containsLabel = false;
                for (HasContainer hasContainer : hasContainers) {
                    if (hasContainer.getKey().equals(T.label.getAccessor())) {
                        SchemaTable hasContainerSchemaTable = SqlgUtil.parseLabelMaybeNoSchema(sqlgGraph, (String) hasContainer.getValue());
                        containsLabel = !hasContainer.getValue().equals(hasContainerSchemaTable.toString());
                    }
                }

                if (!containsLabel) {
                    //add the label
                    HasContainer labelHasContainer = new HasContainer(T.label.getAccessor(), P.eq(schemaTable.toString()));
                    this.hasContainers.add(labelHasContainer);
                    toRemove.add(labelHasContainer);
                }

            }

        }


        Set<SchemaTableTree> result = new HashSet<>();

        List<HasContainer> hasContainerWithoutLabel = this.hasContainers.stream().filter(h -> !h.getKey().equals(T.label.getAccessor())).collect(Collectors.toList());
        List<HasContainer> hasContainerWithLabel = this.hasContainers.stream().filter(h -> h.getKey().equals(T.label.getAccessor())).collect(Collectors.toList());
        if (hasContainerWithLabel.isEmpty()) {
            //this means all vertices or edges
            sqlgGraph.getSchemaManager().getAllTables().forEach((t, p) -> {
                if ((graphStep.getReturnClass().isAssignableFrom(Vertex.class) && t.substring(t.indexOf(".") + 1).startsWith(SchemaManager.VERTEX_PREFIX)) ||
                        (graphStep.getReturnClass().isAssignableFrom(Edge.class) && t.substring(t.indexOf(".") + 1).startsWith(SchemaManager.EDGE_PREFIX))) {

                    SchemaTable schemaTable = SchemaTable.from(sqlgGraph, t, sqlgGraph.getSqlDialect().getPublicSchema());
                    SchemaTableTree schemaTableTree = new SchemaTableTree(
                            sqlgGraph,
                            schemaTable,
                            0,
                            hasContainerWithoutLabel,
                            this.comparators,
                            SchemaTableTree.STEP_TYPE.GRAPH_STEP,
                            ReplacedStep.this.emit,
                            ReplacedStep.this.untilFirst,
                            ReplacedStep.this.emitFirst,
                            ReplacedStep.this.isVertexGraphStep,
                            ReplacedStep.this.labels
                    );

                    result.add(schemaTableTree);

                }
            });
        } else {
            for (HasContainer h : hasContainerWithLabel) {
                //check if the table exist
                SchemaTable schemaTable = SqlgUtil.parseLabelMaybeNoSchema(sqlgGraph, (String) h.getValue());
                String table = (graphStep.getReturnClass().isAssignableFrom(Vertex.class) ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + schemaTable.getTable();
                SchemaTable schemaTableForLabel = SchemaTable.from(sqlgGraph, schemaTable.getSchema() == null ? table : schemaTable.getSchema() + "." + table, sqlgGraph.getSqlDialect().getPublicSchema());
                if (sqlgGraph.getSchemaManager().getAllTables().containsKey(schemaTableForLabel.toString())) {

                    List<HasContainer> hasContainers = new ArrayList<>(hasContainerWithoutLabel);
                    if (groupedIds != null) {
                        List<Long> ids = groupedIds.get(schemaTable);
                        HasContainer idHasContainer = new HasContainer(T.id.getAccessor(), P.within(ids));
                        hasContainers.add(idHasContainer);
                        toRemove.add(idHasContainer);
                    }
                    SchemaTableTree schemaTableTree = new SchemaTableTree(
                            sqlgGraph,
                            schemaTableForLabel,
                            0,
                            hasContainers,
                            this.comparators,
                            SchemaTableTree.STEP_TYPE.GRAPH_STEP,
                            ReplacedStep.this.emit,
                            ReplacedStep.this.untilFirst,
                            ReplacedStep.this.emitFirst,
                            ReplacedStep.this.isVertexGraphStep,
                            ReplacedStep.this.labels
                    );
                    result.add(schemaTableTree);
                }

            }
        }
        this.hasContainers.removeAll(toRemove);
        return result;
    }

    private Map<SchemaTable, List<Long>> groupIdsBySchemaTable(GraphStep graphStep) {
        Map<SchemaTable, List<Long>> groupedIds = new HashMap<>();
        if (graphStep.getIds()[0] instanceof Element) {
            for (Object element : graphStep.getIds()) {
                RecordId recordId = (RecordId) ((Element) element).id();
                List<Long> ids = groupedIds.get(recordId.getSchemaTable());
                if (ids == null) {
                    ids = new ArrayList<>();
                    groupedIds.put(recordId.getSchemaTable(), ids);
                }
                ids.add(recordId.getId());
            }
        } else {
            for (Object id : graphStep.getIds()) {
                RecordId recordId;
                if (id instanceof RecordId) {
                    recordId = (RecordId) id;
                } else {
                    recordId = RecordId.from(id);
                }
                List<Long> ids = groupedIds.get(recordId.getSchemaTable());
                if (ids == null) {
                    ids = new ArrayList<>();
                    groupedIds.put(recordId.getSchemaTable(), ids);
                }
                ids.add(recordId.getId());

            }
        }
        return groupedIds;
    }

    public AbstractStep<S, E> getStep() {
        return step;
    }

    public boolean isEmit() {
        return emit;
    }

    public void setEmit(boolean emit) {
        this.emit = emit;
    }

    public boolean isUntilFirst() {
        return untilFirst;
    }

    public void setUntilFirst(boolean untilFirst) {
        this.untilFirst = untilFirst;
    }

    public boolean isEmitFirst() {
        return emitFirst;
    }

    public void setEmitFirst(boolean emitFirst) {
        this.emitFirst = emitFirst;
    }

    public int getDepth() {
        return depth;
    }
}
