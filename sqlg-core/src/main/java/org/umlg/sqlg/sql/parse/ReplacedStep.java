package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.umlg.sqlg.strategy.BaseStrategy;
import org.umlg.sqlg.strategy.SqlgComparatorHolder;
import org.umlg.sqlg.strategy.SqlgRangeHolder;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.ForeignKey;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.structure.T.id;
import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * Date: 2015/06/27
 * Time: 6:05 PM
 */
public class ReplacedStep<S, E> {

    private Topology topology;
    private AbstractStep<S, E> step;
    private Set<String> labels;
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final List<HasContainer> idHasContainers = new ArrayList<>();
    private final List<HasContainer> labelHasContainers = new ArrayList<>();
    private final List<AndOrHasContainer> andOrHasContainers = new ArrayList<>();
    private final SqlgComparatorHolder sqlgComparatorHolder = new SqlgComparatorHolder();
    private final List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> dbComparators = new ArrayList<>();
    /**
     * range limitation if any
     */
    private SqlgRangeHolder sqlgRangeHolder;
    //This indicates the distanced of the replaced steps from the starting step. i.e. g.V(1).out().out().out() will be 0,1,2 for the 3 outs
    private int depth;
    private boolean emit;
    private boolean untilFirst;
    //indicate left join, coming from optional step optimization
    private boolean leftJoin;
    private boolean fake;
    private boolean joinToLeftJoin;
    private boolean drop;

    /**
     * The reducing barrier's aggregate function. i.e. max, min, mean...
     * The pair's left is the aggregate function and the right is the columns over which to aggregate.
     */
    private Pair<String, List<String>> aggregateFunction;
    private List<String> groupBy;

    /**
     * restrict properties to only a subset if not null
     */
    private Set<String> restrictedProperties = null;

    /**
     * If the query is a for sqlg_schema only. i.e. sqlgGraph.topology().V()....
     */
    private boolean isForSqlgSchema;

    //IDStep, i.e. only return the element's id.
    private boolean idOnly = false;

    private ReplacedStep() {
    }

    /**
     * Used for SqlgVertexStepStrategy. It is a fake ReplacedStep to simulate the incoming vertex from which the traversal continues.
     *
     * @param topology
     * @return
     */
    public static ReplacedStep from(Topology topology) {
        ReplacedStep replacedStep = new ReplacedStep<>();
        replacedStep.step = null;
        replacedStep.labels = new HashSet<>();
        replacedStep.topology = topology;
        replacedStep.fake = true;
        return replacedStep;
    }

    public static <S, E> ReplacedStep from(Topology topology, AbstractStep<S, E> step, int pathCount) {
        ReplacedStep replacedStep = new ReplacedStep<>();
        replacedStep.step = step;
        replacedStep.labels = step.getLabels().stream().map(l -> pathCount + BaseStrategy.PATH_LABEL_SUFFIX + l).collect(Collectors.toSet());
        replacedStep.topology = topology;
        replacedStep.fake = false;
        return replacedStep;
    }

    boolean isFake() {
        return fake;
    }

    List<HasContainer> getHasContainers() {
        return this.hasContainers;
    }

    public List<AndOrHasContainer> getAndOrHasContainers() {
        return this.andOrHasContainers;
    }

    public SqlgComparatorHolder getSqlgComparatorHolder() {
        return this.sqlgComparatorHolder;
    }

    public List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> getDbComparators() {
        return this.dbComparators;
    }

    public void addLabel(String label) {
        this.labels.add(label);
    }

    public Set<String> getLabels() {
        return this.labels;
    }

    public boolean hasLabels() {
        return !this.labels.isEmpty();
    }

    private Set<SchemaTableTree> appendPath(SchemaTableTree schemaTableTree) {
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
        Preconditions.checkState(schemaTableTree.getDirection() != null, "SchemaTableTree must have an Direction to execute the EdgeOtherVertexStep");
        return calculatePathFromEdgeToVertex(schemaTableTree, schemaTableTree.getSchemaTable(), (schemaTableTree.getDirection() == Direction.IN ? Direction.OUT : Direction.IN));
    }

    private Set<SchemaTableTree> appendPathForVertexStep(SchemaTableTree schemaTableTree) {
        Preconditions.checkArgument(schemaTableTree.getSchemaTable().isVertexTable(), "Expected a Vertex table found " + schemaTableTree.getSchemaTable().getTable());

        Map<String, Map<String, PropertyType>> filteredAllTables = this.topology.getAllTables(this.isForSqlgSchema);

        Set<SchemaTableTree> result = new HashSet<>();
        Pair<Set<SchemaTable>, Set<SchemaTable>> inAndOutLabelsFromCurrentPosition = this.topology.getTableLabels(schemaTableTree.getSchemaTable());

        VertexStep<? extends Element> vertexStep = (VertexStep<? extends Element>) this.step;
        String[] edgeLabels = vertexStep.getEdgeLabels();
        Direction direction = vertexStep.getDirection();
        Class<? extends Element> elementClass = vertexStep.getReturnClass();

        Set<SchemaTable> inEdgeLabels = inAndOutLabelsFromCurrentPosition != null ? inAndOutLabelsFromCurrentPosition.getLeft() : new HashSet<>();
        Set<SchemaTable> outEdgeLabels = inAndOutLabelsFromCurrentPosition != null ? inAndOutLabelsFromCurrentPosition.getRight() : new HashSet<>();
        Set<SchemaTable> inEdgeLabelsToTraversers;
        Set<SchemaTable> outEdgeLabelsToTraversers;
        switch (vertexStep.getDirection()) {
            case IN:
                inEdgeLabelsToTraversers = filterVertexStepOnEdgeLabels(inEdgeLabels, edgeLabels);
                outEdgeLabelsToTraversers = new HashSet<>();
                break;
            case OUT:
                outEdgeLabelsToTraversers = filterVertexStepOnEdgeLabels(outEdgeLabels, edgeLabels);
                inEdgeLabelsToTraversers = new HashSet<>();
                break;
            case BOTH:
                inEdgeLabelsToTraversers = edgeLabels.length > 0 ? filterVertexStepOnEdgeLabels(inEdgeLabels, edgeLabels) : inEdgeLabels;
                outEdgeLabelsToTraversers = edgeLabels.length > 0 ? filterVertexStepOnEdgeLabels(outEdgeLabels, edgeLabels) : outEdgeLabels;
                break;
            default:
                throw new IllegalStateException("Unknown direction " + direction.name());
        }

        Map<SchemaTable, List<Multimap<BiPredicate<?, ?>, RecordId>>> groupedIds = groupIdsBySchemaTable();

        //Each labelToTravers more than the first one forms a new distinct path
        for (SchemaTable inEdgeLabelToTravers : inEdgeLabelsToTraversers) {
            if (elementClass.isAssignableFrom(Edge.class)) {
                if (passesLabelHasContainers(this.topology.getSqlgGraph(), false, inEdgeLabelToTravers.toString()) &&
                        passesRestrictedProperties(filteredAllTables.get(inEdgeLabelToTravers.toString()))) {

                    SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                            inEdgeLabelToTravers,
                            Direction.IN,
                            elementClass,
                            this,
                            this.labels);

                    SchemaTable schemaTable = SchemaTable.from(this.topology.getSqlgGraph(), inEdgeLabelToTravers.toString());
                    List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIs = groupedIds.get(schemaTable.withOutPrefix());
                    addIdHasContainers(schemaTableTreeChild, biPredicateRecordIs);

                    result.add(schemaTableTreeChild);
                }
            } else {
                Map<String, Set<ForeignKey>> edgeForeignKeys = this.topology.getEdgeForeignKeys();
                Set<ForeignKey> foreignKeys = edgeForeignKeys.get(inEdgeLabelToTravers.toString());
                boolean first = true;
                SchemaTableTree schemaTableTreeChild = null;
                //Use this set to filter the foreignKeys.
                //For user defined ids many columns can be used as the primary keys.
                //i.e. many __I columns.
                //We are only interested here in the distinct SchemaTables.
                Set<SchemaTable> schemaTables = new HashSet<>();
                for (ForeignKey foreignKey : foreignKeys) {
                    if (foreignKey.isOut()) {
                        String foreignKeySchema = foreignKey.getSchemaTable().getSchema();
                        String foreignKeyTable = foreignKey.getSchemaTable().getTable();
                        SchemaTable schemaTableTo = SchemaTable.of(foreignKeySchema, VERTEX_PREFIX + SqlgUtil.removeTrailingOutId(foreignKeyTable));
                        if (schemaTables.add(schemaTableTo) &&
                                passesLabelHasContainers(this.topology.getSqlgGraph(), true, schemaTableTo.toString()) &&
                                passesRestrictedProperties(filteredAllTables.get(schemaTableTo.toString()))) {

                            if (first) {
                                first = false;
                                schemaTableTreeChild = schemaTableTree.addChild(
                                        inEdgeLabelToTravers,
                                        Direction.IN,
                                        elementClass,
                                        this,
                                        Collections.emptySet());
                            }
                            result.addAll(calculatePathFromVertexToEdge(schemaTableTreeChild, schemaTableTo, Direction.IN, groupedIds));
                        }
                    }
                }
            }
        }

        for (SchemaTable outEdgeLabelToTravers : outEdgeLabelsToTraversers) {
            if (elementClass.isAssignableFrom(Edge.class)) {

                if (passesLabelHasContainers(this.topology.getSqlgGraph(), false, outEdgeLabelToTravers.toString()) &&
                        passesRestrictedProperties(filteredAllTables.get(outEdgeLabelToTravers.toString()))) {

                    SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                            outEdgeLabelToTravers,
                            Direction.OUT,
                            elementClass,
                            this,
                            this.labels);

                    SchemaTable schemaTable = SchemaTable.from(this.topology.getSqlgGraph(), outEdgeLabelToTravers.toString());
                    List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIds = groupedIds.get(schemaTable.withOutPrefix());
                    addIdHasContainers(schemaTableTreeChild, biPredicateRecordIds);

                    result.add(schemaTableTreeChild);
                }
            } else {
                Map<String, Set<ForeignKey>> edgeForeignKeys = this.topology.getEdgeForeignKeys();
                Set<ForeignKey> foreignKeys = edgeForeignKeys.get(outEdgeLabelToTravers.toString());
                boolean first = true;
                SchemaTableTree schemaTableTreeChild = null;
                //Use this set to filter the foreignKeys.
                //For user defined ids many columns can be used as the primary keys.
                //i.e. many __I columns.
                //We are only interested here in the distinct SchemaTables.
                for (ForeignKey foreignKey : foreignKeys) {
                    if (foreignKey.isIn()) {
                        String foreignKeySchema = foreignKey.getSchemaTable().getSchema();
                        String foreignKeyTable = foreignKey.getSchemaTable().getTable();
                        SchemaTable schemaTableTo = SchemaTable.of(foreignKeySchema, VERTEX_PREFIX + SqlgUtil.removeTrailingInId(foreignKeyTable));
                        if (passesLabelHasContainers(this.topology.getSqlgGraph(), true, schemaTableTo.toString()) &&
                                passesRestrictedProperties(filteredAllTables.get(schemaTableTo.toString()))) {

                            if (first) {
                                first = false;
                                schemaTableTreeChild = schemaTableTree.addChild(
                                        outEdgeLabelToTravers,
                                        Direction.OUT,
                                        elementClass,
                                        this,
                                        Collections.emptySet());
                            }
                            result.addAll(calculatePathFromVertexToEdge(schemaTableTreeChild, schemaTableTo, Direction.OUT, groupedIds));
                        }
                    }
                }
            }
        }
        return result;
    }

    private Set<SchemaTableTree> calculatePathFromEdgeToVertex(SchemaTableTree schemaTableTree, SchemaTable labelToTravers, Direction direction) {
        Preconditions.checkArgument(labelToTravers.isEdgeTable());

        Map<SchemaTable, List<Multimap<BiPredicate<?, ?>, RecordId>>> groupedIds = groupIdsBySchemaTable();

        Set<SchemaTableTree> result = new HashSet<>();
        Map<String, Set<ForeignKey>> edgeForeignKeys = this.topology.getEdgeForeignKeys();
        //join from the edge table to the incoming vertex table
        Set<ForeignKey> foreignKeys = edgeForeignKeys.get(labelToTravers.toString());
        //Every foreignKey for the given direction must be joined on

        //Use this set to filter the foreignKeys.
        //For user defined ids many columns can be used as the primary keys.
        //i.e. many __I columns.
        //We are only interested here in the distinct SchemaTables.
        for (ForeignKey foreignKey : foreignKeys) {

            String foreignKeySchema = foreignKey.getSchemaTable().getSchema();
            String foreignKeyTable = foreignKey.getSchemaTable().getTable();

            if ((direction == Direction.BOTH || direction == Direction.OUT) && foreignKey.isOut()) {
                SchemaTable schemaTable = SchemaTable.of(foreignKeySchema, VERTEX_PREFIX + SqlgUtil.removeTrailingOutId(foreignKeyTable));
                if (passesLabelHasContainers(this.topology.getSqlgGraph(), true, schemaTable.toString())) {
                    SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                            schemaTable,
                            Direction.OUT,
                            this,
                            this.labels
                    );
                    List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIs = groupedIds.get(schemaTable.withOutPrefix());
                    addIdHasContainers(schemaTableTreeChild, biPredicateRecordIs);
                    result.add(schemaTableTreeChild);
                }
            }
            if ((direction == Direction.BOTH || direction == Direction.IN) && foreignKey.isIn()) {
                SchemaTable schemaTable = SchemaTable.of(foreignKeySchema, VERTEX_PREFIX + SqlgUtil.removeTrailingInId(foreignKeyTable));
                if (passesLabelHasContainers(this.topology.getSqlgGraph(), true, schemaTable.toString())) {
                    SchemaTableTree schemaTableTreeChild = schemaTableTree.addChild(
                            schemaTable,
                            Direction.IN,
                            this,
                            this.labels
                    );
                    List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIs = groupedIds.get(schemaTable.withOutPrefix());
                    addIdHasContainers(schemaTableTreeChild, biPredicateRecordIs);
                    result.add(schemaTableTreeChild);
                }
            }
        }
        return result;
    }

    private Set<SchemaTableTree> calculatePathFromVertexToEdge(
            SchemaTableTree schemaTableTree,
            SchemaTable schemaTableTo,
            Direction direction,
            Map<SchemaTable, List<Multimap<BiPredicate<?, ?>, RecordId>>> groupedIds) {

        Set<SchemaTableTree> result = new HashSet<>();
        //add the child for schemaTableTo to the tree
        SchemaTableTree schemaTableTree1 = schemaTableTree.addChild(
                schemaTableTo,
                direction,
                Vertex.class,
                this,
                this.labels
        );
        SchemaTable schemaTable = SchemaTable.from(this.topology.getSqlgGraph(), schemaTableTo.toString());
        List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIs = groupedIds.get(schemaTable.withOutPrefix());
        addIdHasContainers(schemaTableTree1, biPredicateRecordIs);
        result.add(schemaTableTree1);
        return result;
    }

    private void addIdHasContainers(SchemaTableTree schemaTableTree1, List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIds) {
        if (biPredicateRecordIds != null) {
            for (Multimap<BiPredicate<?, ?>, RecordId> biPredicateRecordId : biPredicateRecordIds) {
                for (BiPredicate<?, ?> biPredicate : biPredicateRecordId.keySet()) {
                    Collection<RecordId> recordIds = biPredicateRecordId.get(biPredicate);
                    HasContainer idHasContainer;
                    //id hasContainers are only optimized for BaseStrategy.SUPPORTED_ID_BI_PREDICATE within, without, eq, neq
                    if (biPredicate == Contains.without || biPredicate == Contains.within) {
                        idHasContainer = new HasContainer(T.id.getAccessor(), P.test(biPredicate, recordIds));
                        schemaTableTree1.getHasContainers().add(idHasContainer);
                    } else {
                        Preconditions.checkState(biPredicate == Compare.eq || biPredicate == Compare.neq);
                        for (RecordId recordId : recordIds) {
                            idHasContainer = new HasContainer(T.id.getAccessor(), P.test(biPredicate, recordId));
                            schemaTableTree1.getHasContainers().add(idHasContainer);
                        }
                    }
                }
            }
        }
    }

    Set<SchemaTableTree> calculatePathForStep(Set<SchemaTableTree> schemaTableTrees) {
        Set<SchemaTableTree> result = new HashSet<>();
        for (SchemaTableTree schemaTableTree : schemaTableTrees) {
            result.addAll(this.appendPath(schemaTableTree));
        }
        return result;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    private Set<SchemaTable> filterVertexStepOnEdgeLabels(Set<SchemaTable> labels, String[] edgeLabels) {
        Set<SchemaTable> result = new HashSet<>();
        List<String> edges = Arrays.asList(edgeLabels);
        for (SchemaTable label : labels) {
            if (!label.getTable().startsWith(EDGE_PREFIX)) {
                throw new IllegalStateException("Expected label to start with " + EDGE_PREFIX);
            }
            String rawLabel = label.getTable().substring(EDGE_PREFIX.length());
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

    @Override
    public String toString() {
        if (this.step != null) {
            return this.step + " :: " + this.hasContainers + " :: leftJoin = " + this.leftJoin + " :: joinToLeftJoin = " + this.joinToLeftJoin;
        } else {
            return "fakeStep :: " + this.hasContainers + " :: leftJoin = " + this.leftJoin + " :: joinToLeftJoin = " + this.joinToLeftJoin;
        }
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

    public boolean isEdgeOtherVertexStep() {
        return this.step instanceof EdgeOtherVertexStep;
    }

    /**
     * Calculates the root labels from which to start the query construction.
     * <p>
     * The hasContainers at this stage contains the {@link TopologyStrategy} from or without hasContainer.
     * After doing the filtering it must be removed from the hasContainers as it must not partake in sql generation.
     *
     * @return A set of SchemaTableTree. A SchemaTableTree for each root label.
     */
    Set<SchemaTableTree> getRootSchemaTableTrees(SqlgGraph sqlgGraph, int replacedStepDepth) {
        Preconditions.checkState(this.isGraphStep(), "ReplacedStep must be for a GraphStep!");
        Set<SchemaTableTree> result = new HashSet<>();
        final GraphStep graphStep = (GraphStep) this.step;
        @SuppressWarnings("unchecked") final boolean isVertex = graphStep.getReturnClass().isAssignableFrom(Vertex.class);
        final boolean isEdge = !isVertex;

        //RecordIds grouped by SchemaTable
        Map<SchemaTable, List<Multimap<BiPredicate<?, ?>, RecordId>>> groupedIds = null;
        if (!this.idHasContainers.isEmpty()) {
            groupedIds = groupIdsBySchemaTable();
        }

        //All tables depending on the strategy, topology tables only or the rest.
        Map<String, Map<String, PropertyType>> filteredAllTables = this.topology.getAllTables(this.isForSqlgSchema);

        //Optimization for the simple case of only one label specified.
        if (isVertex && this.labelHasContainers.size() == 1 && this.labelHasContainers.get(0).getBiPredicate() == Compare.eq) {
            HasContainer labelHasContainer = this.labelHasContainers.get(0);
            String table = (String) labelHasContainer.getValue();
            SchemaTable schemaTableWithPrefix = SchemaTable.from(sqlgGraph, table).withPrefix(VERTEX_PREFIX);
            if (filteredAllTables.containsKey(schemaTableWithPrefix.toString())) {
                collectSchemaTableTrees(sqlgGraph, replacedStepDepth, result, groupedIds, schemaTableWithPrefix.toString());
            }
        } else {
            for (String table : filteredAllTables.keySet()) {
                //if graphStep's return class is Vertex ignore all edges and vice versa.
                if ((isVertex && table.substring(table.indexOf(".") + 1).startsWith(VERTEX_PREFIX)) ||
                        (isEdge && table.substring(table.indexOf(".") + 1).startsWith(EDGE_PREFIX))) {

                    if (passesLabelHasContainers(sqlgGraph, isVertex, table)) {
                        collectSchemaTableTrees(sqlgGraph, replacedStepDepth, result, groupedIds, table);
                    }
                }
            }
        }
        return result;
    }

    private void collectSchemaTableTrees(
            SqlgGraph sqlgGraph,
            int replacedStepDepth,
            Set<SchemaTableTree> result,
            Map<SchemaTable, List<Multimap<BiPredicate<?, ?>, RecordId>>> groupedIds,
            String table) {

        SchemaTable schemaTable = SchemaTable.from(sqlgGraph, table);

        List<HasContainer> schemaTableTreeHasContainers = new ArrayList<>(this.hasContainers);

        if (groupedIds != null) {
            List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIds = groupedIds.get(schemaTable.withOutPrefix());
            if (biPredicateRecordIds != null) {
                for (Multimap<BiPredicate<?, ?>, RecordId> biPredicateRecordId : biPredicateRecordIds) {
                    for (BiPredicate<?, ?> biPredicate : biPredicateRecordId.keySet()) {
                        Collection<RecordId> recordIds = biPredicateRecordId.get(biPredicate);
                        HasContainer idHasContainer;
                        //id hasContainers are only optimized for BaseStrategy.SUPPORTED_ID_BI_PREDICATE within, without, eq, neq
                        if (biPredicate == Contains.without || biPredicate == Contains.within) {
                            idHasContainer = new HasContainer(T.id.getAccessor(), P.test(biPredicate, recordIds));
                            schemaTableTreeHasContainers.add(idHasContainer);
                        } else {
                            Preconditions.checkState(biPredicate == Compare.eq || biPredicate == Compare.neq);
                            for (RecordId recordId : recordIds) {
                                idHasContainer = new HasContainer(T.id.getAccessor(), P.test(biPredicate, recordId));
                                schemaTableTreeHasContainers.add(idHasContainer);
                            }
                        }
                    }
                }
            }
        }
        SchemaTableTree schemaTableTree = new SchemaTableTree(
                sqlgGraph,
                schemaTable,
                0,
                schemaTableTreeHasContainers,
                this.andOrHasContainers,
                this.sqlgComparatorHolder,
                this.sqlgComparatorHolder.getComparators(),
                this.sqlgRangeHolder,
                SchemaTableTree.STEP_TYPE.GRAPH_STEP,
                this.emit,
                this.untilFirst,
                this.leftJoin,
                this.drop,
                replacedStepDepth,
                this.labels,
                aggregateFunction,
                groupBy,
                idOnly
        );
        schemaTableTree.setRestrictedProperties(getRestrictedProperties());
        result.add(schemaTableTree);
    }

    @SuppressWarnings("unchecked")
    private boolean passesLabelHasContainers(SqlgGraph sqlgGraph, boolean isVertex, String table) {
        return this.labelHasContainers.stream().allMatch(h -> {
            BiPredicate biPredicate = h.getBiPredicate();
            Object predicateValue = h.getValue();
            if (predicateValue instanceof Collection) {
                Collection<String> tableWithPrefixes = new ArrayList<>();
                Collection<String> edgeTableWithoutSchemaAndPrefixes = new ArrayList<>();
                @SuppressWarnings("unchecked")
                Collection<String> predicateValues = (Collection<String>) predicateValue;
                SchemaTable schemaTableWithOutPrefix = SchemaTable.from(sqlgGraph, table).withOutPrefix();
                for (String value : predicateValues) {
                    if (!isVertex && !value.contains(".")) {
                        //edges usually don't have schema, so we're matching any table with any schema if we weren't given any
                        edgeTableWithoutSchemaAndPrefixes.add(value);
                    } else {
                        SchemaTable predicateValueAsSchemaTableWithPrefix = SchemaTable.from(sqlgGraph, value).withPrefix(isVertex ? VERTEX_PREFIX : EDGE_PREFIX);
                        tableWithPrefixes.add(predicateValueAsSchemaTableWithPrefix.toString());
                    }
                }
                if (edgeTableWithoutSchemaAndPrefixes.isEmpty()) {
                    return biPredicate.test(table, tableWithPrefixes);
                } else if (tableWithPrefixes.isEmpty()) {
                    return biPredicate.test(schemaTableWithOutPrefix.getTable(), edgeTableWithoutSchemaAndPrefixes);
                } else {
                    return biPredicate.test(table, tableWithPrefixes) || biPredicate.test(schemaTableWithOutPrefix.getTable(), edgeTableWithoutSchemaAndPrefixes);
                }
            } else {
                Preconditions.checkState(predicateValue instanceof String, "Label HasContainer's value must be an Collection of Strings or a String. Found " + predicateValue.getClass().toString());
                if (!isVertex && !((String) predicateValue).contains(".")) {
                    //edges usually don't have schema, so we're matching any table with any schema if we weren't given any
                    SchemaTable schemaTableWithOutPrefix = SchemaTable.from(sqlgGraph, table).withOutPrefix();
                    return biPredicate.test(schemaTableWithOutPrefix.getTable(), predicateValue);
                } else {
                    SchemaTable predicateValueAsSchemaTableWithPrefix = SchemaTable.from(sqlgGraph, (String) predicateValue).withPrefix(isVertex ? VERTEX_PREFIX : EDGE_PREFIX);
                    return biPredicate.test(table, predicateValueAsSchemaTableWithPrefix.toString());
                }
            }
        });
    }

    private boolean passesRestrictedProperties(Map<String, PropertyType> propertyTypeMap) {
        if (this.isIdOnly() || this.restrictedProperties == null) {
            return true;
        }
        for (String restrictedProperty : this.restrictedProperties) {
            //or logic, if any property is present its a go
            if (!Graph.Hidden.isHidden(restrictedProperty) && propertyTypeMap.containsKey(restrictedProperty)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Groups the idHasContainers by SchemaTable.
     * Each SchemaTable has a list representing the idHasContainers with the relevant BiPredicate and RecordId
     *
     * @return
     */
    private Map<SchemaTable, List<Multimap<BiPredicate<?, ?>, RecordId>>> groupIdsBySchemaTable() {
        Map<SchemaTable, List<Multimap<BiPredicate<?, ?>, RecordId>>> result = new HashMap<>();
        for (HasContainer idHasContainer : this.idHasContainers) {

            Map<SchemaTable, Boolean> newHasContainerMap = new HashMap<>();
            @SuppressWarnings("unchecked")
            P<Object> idPredicate = (P<Object>) idHasContainer.getPredicate();
            BiPredicate<?, ?> biPredicate = idHasContainer.getBiPredicate();
            //This is statement is for g.V().hasId(Collection) where the logic is actually P.within not P.eq
            if (biPredicate == Compare.eq && idPredicate.getValue() instanceof Collection && ((Collection) idPredicate.getValue()).size() > 1) {
                biPredicate = Contains.within;
            }
            Multimap<BiPredicate<?, ?>, RecordId> biPredicateRecordIdMultimap;
            if (idPredicate.getValue() instanceof Collection) {

                @SuppressWarnings("unchecked")
                Collection<Object> ids = (Collection<Object>) idPredicate.getValue();
                for (Object id : ids) {
                    RecordId recordId = RecordId.from(id);
                    List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIdList = result.get(recordId.getSchemaTable());
                    Boolean newHasContainer = newHasContainerMap.get(recordId.getSchemaTable());
                    if (biPredicateRecordIdList == null) {
                        biPredicateRecordIdList = new ArrayList<>();
                        biPredicateRecordIdMultimap = LinkedListMultimap.create();
                        biPredicateRecordIdList.add(biPredicateRecordIdMultimap);
                        result.put(recordId.getSchemaTable(), biPredicateRecordIdList);
                        newHasContainerMap.put(recordId.getSchemaTable(), false);
                    } else if (newHasContainer == null) {
                        biPredicateRecordIdMultimap = LinkedListMultimap.create();
                        biPredicateRecordIdList.add(biPredicateRecordIdMultimap);
                        newHasContainerMap.put(recordId.getSchemaTable(), false);
                    }
                    biPredicateRecordIdMultimap = biPredicateRecordIdList.get(biPredicateRecordIdList.size() - 1);
                    biPredicateRecordIdMultimap.put(biPredicate, recordId);
                }
            } else {
                Object id = idPredicate.getValue();
                RecordId recordId = RecordId.from(id);
                List<Multimap<BiPredicate<?, ?>, RecordId>> biPredicateRecordIdList = result.computeIfAbsent(recordId.getSchemaTable(), k -> new ArrayList<>());
                biPredicateRecordIdMultimap = LinkedListMultimap.create();
                biPredicateRecordIdList.add(biPredicateRecordIdMultimap);
                biPredicateRecordIdMultimap.put(biPredicate, recordId);
            }
        }
        return result;
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

    public void setLeftJoin(boolean leftJoin) {
        this.leftJoin = leftJoin;
    }

    public boolean isLeftJoin() {
        return leftJoin;
    }

    public boolean isDrop() {
        return drop;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isJoinToLeftJoin() {
        return joinToLeftJoin;
    }

    public boolean isUntilFirst() {
        return untilFirst;
    }

    public void setUntilFirst(boolean untilFirst) {
        this.untilFirst = untilFirst;
    }

    public int getDepth() {
        return depth;
    }

    /**
     * Each id is for a specific label, add the label to the {@link ReplacedStep#labelHasContainers}
     *
     * @param hasContainer A hasContainer with {@link T#id} as key for this step. Copied from the original step.
     */
    public void addIdHasContainer(HasContainer hasContainer) {
        Preconditions.checkState(BaseStrategy.SUPPORTED_ID_BI_PREDICATE.contains(hasContainer.getBiPredicate()), "Only " + BaseStrategy.SUPPORTED_ID_BI_PREDICATE.toString() + " is supported, found " + hasContainer.getBiPredicate().getClass().toString());
        Object rawId = hasContainer.getValue();
        if (rawId instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> ids = (Collection<Object>) rawId;
            Set<String> idLabels = new HashSet<>();
            for (Object id : ids) {
                if (id instanceof RecordId) {
                    RecordId recordId = (RecordId) id;
                    idLabels.add(recordId.getSchemaTable().toString());
                } else if (id instanceof Element) {
                    SqlgElement sqlgElement = (SqlgElement) id;
                    RecordId recordId = (RecordId) sqlgElement.id();
                    idLabels.add(recordId.getSchemaTable().toString());
                } else if (id instanceof String) {
                    RecordId recordId = RecordId.from(id);
                    idLabels.add(recordId.getSchemaTable().toString());
                } else {
                    throw new IllegalStateException("id must be an Element or a RecordId, found " + id.getClass().toString());
                }
            }
            if (hasContainer.getBiPredicate() == Contains.without) {
                //The id's label needs to be added to previous labelHasContainers labels.
                //without indicates that the label needs to be queried along with the rest, 'or' logic rather than 'and'.
                if (!this.labelHasContainers.isEmpty()) {
                    Object previousHasContainerLabels = this.labelHasContainers.get(this.labelHasContainers.size() - 1).getValue();
                    List<String> mergedLabels;
                    if (previousHasContainerLabels instanceof Collection) {
                        @SuppressWarnings("unchecked") Collection<String> labels = (Collection<String>) previousHasContainerLabels;
                        mergedLabels = new ArrayList<>(labels);
                    } else {
                        String label = (String) previousHasContainerLabels;
                        mergedLabels = new ArrayList<>();
                        mergedLabels.add(label);
                    }
                    mergedLabels.addAll(idLabels);
                    this.labelHasContainers.set(this.labelHasContainers.size() - 1, new HasContainer(T.label.getAccessor(), P.within(mergedLabels)));
                }
            } else {
                this.labelHasContainers.add(new HasContainer(T.label.getAccessor(), P.within(idLabels)));
            }
        } else {
            RecordId recordId;
            if (rawId instanceof RecordId) {
                recordId = (RecordId) rawId;
            } else if (rawId instanceof Element) {
                SqlgElement sqlgElement = (SqlgElement) rawId;
                recordId = (RecordId) sqlgElement.id();
            } else if (rawId instanceof String) {
                recordId = RecordId.from(rawId);
            } else {
                throw new IllegalStateException("id must be an Element or a RecordId, found " + id.getClass().toString());
            }
            if (hasContainer.getBiPredicate() == Compare.neq) {
                if (!this.labelHasContainers.isEmpty()) {
                    Object previousHasContainerLabels = this.labelHasContainers.get(this.labelHasContainers.size() - 1).getValue();
                    List<String> mergedLabels;
                    if (previousHasContainerLabels instanceof Collection) {
                        @SuppressWarnings("unchecked") Collection<String> labels = (Collection<String>) previousHasContainerLabels;
                        mergedLabels = new ArrayList<>(labels);
                    } else {
                        String label = (String) previousHasContainerLabels;
                        mergedLabels = new ArrayList<>();
                        mergedLabels.add(label);
                    }
                    mergedLabels.add(recordId.getSchemaTable().toString());
                    this.labelHasContainers.set(this.labelHasContainers.size() - 1, new HasContainer(T.label.getAccessor(), P.within(mergedLabels)));
                }
            } else {
                this.labelHasContainers.add(new HasContainer(T.label.getAccessor(), P.eq(recordId.getSchemaTable().toString())));
            }
        }
        this.idHasContainers.add(hasContainer);
    }

    /**
     * @param hasContainer A hasContainers with {@link T#label} as key for this step. Copied from the original step.
     */
    public void addLabelHasContainer(HasContainer hasContainer) {
        Preconditions.checkState(hasContainer.getKey().equals(T.label.getAccessor()), "ReplacedStep.addLabelHasContainer may only be called for HasContainers with T.label as key.");
        Preconditions.checkState(BaseStrategy.SUPPORTED_LABEL_BI_PREDICATE.contains(hasContainer.getBiPredicate()));
        this.labelHasContainers.add(hasContainer);
    }

    public List<HasContainer> getLabelHasContainers() {
        return labelHasContainers;
    }

    /**
     * @param hasContainer A hasContainer for this step. Copied from the original step.
     */
    public void addHasContainer(HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    public void addAndOrHasContainer(AndOrHasContainer andOrHasContainer) {
        this.andOrHasContainers.add(andOrHasContainer);
    }

    public SqlgRangeHolder getSqlgRangeHolder() {
        return this.sqlgRangeHolder;
    }

    public void setSqlgRangeHolder(SqlgRangeHolder sqlgRangeHolder) {
        this.sqlgRangeHolder = sqlgRangeHolder;
    }

    public boolean hasRange() {
        return this.getSqlgRangeHolder() != null;
    }

    public boolean applyInStep() {
        return this.getSqlgRangeHolder().isApplyInStep();
    }

    public void markAsJoinToLeftJoin() {
        this.joinToLeftJoin = true;
    }

    public void markAsDrop() {
        this.drop = true;
    }

    public Set<String> getRestrictedProperties() {
        return restrictedProperties;
    }

    public void setRestrictedProperties(Set<String> restrictedColumns) {
        this.restrictedProperties = restrictedColumns;
    }

    public void markForSqlgSchema() {
        this.isForSqlgSchema = true;
    }

    public boolean isForSqlgSchema() {
        return isForSqlgSchema;
    }

    public Pair<String, List<String>> getAggregateFunction() {
        return this.aggregateFunction;
    }

    public boolean hasAggregateFunction() {
        return this.aggregateFunction != null;
    }

    public void setAggregateFunction(Pair<String, List<String>> aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public boolean isIdOnly() {
        return this.idOnly;
    }

    public void setIdOnly(boolean idOnly) {
        this.idOnly = idOnly;
    }
}
