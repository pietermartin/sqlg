package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.umlg.sqlg.predicate.Existence;
import org.umlg.sqlg.predicate.FullText;
import org.umlg.sqlg.strategy.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.util.SqlgUtil;

import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.structure.T.label;
import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * Date: 2015/01/08
 * Time: 7:06 AM
 */
public class SchemaTableTree {
    public static final String ALIAS_SEPARATOR = "~&~";
    private static final String CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT = "constructSql may only be called on the root object";
    private static final String WITHIN = "within";
    private static final String WITHOUT = "without";
    //stepDepth indicates the depth of the replaced steps. i.e. v1.out().out().out() existVertexLabel stepDepth 0,1,2,3
    private final int stepDepth;
    private final SchemaTable schemaTable;
    private final List<SchemaTableTree> children = new ArrayList<>();
    private final SqlgGraph sqlgGraph;
    //leafNodes is only set on the root node;
    private final List<SchemaTableTree> leafNodes = new ArrayList<>();
    private final List<HasContainer> additionalPartitionHasContainers = new ArrayList<>();
    //This represents all tables filtered by TopologyStrategy
    private final Map<String, Map<String, PropertyDefinition>> filteredAllTables;
    private final int replacedStepDepth;
    private final boolean hasIDPrimaryKey;
    private final SchemaTableTree parent;
    //The root node does not have a direction. For the other nodes it indicates the direction from its parent to it.
    private final Direction direction;
    private final STEP_TYPE stepType;
    private final List<HasContainer> hasContainers;
    private final List<AndOrHasContainer> andOrHasContainers;
    private final SqlgComparatorHolder sqlgComparatorHolder;
    private final List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> dbComparators;
    //untilFirst is for the repeatStep optimization
    private final boolean untilFirst;
    private final boolean emit;
    //left join, as required by the optimized ChooseStep via the optional step
    private final boolean optionalLeftJoin;
    //indicates NotStep
    private final boolean outerLeftJoin;
    private final ListOrderedSet<String> identifiers;
    private final String distributionColumn;
    private final boolean localStep;
    /**
     * Indicates the DropStep.
     */
    private final boolean drop;
    //Indicates that the SchemaTableTree is a child of a SqlgLocalBarrierStep
    private final boolean localBarrierStep;
    /**
     * range limitation, if any
     */
    private final SqlgRangeHolder sqlgRangeHolder;
    private final Set<String> restrictedProperties = new HashSet<>();

    private final List<String> groupBy;
    private final Pair<String, List<String>> aggregateFunction;

    //Indicates a IdStep, only the element id must be returned.
    private final boolean idOnly;
    private final RecursiveRepeatStepConfig recursiveRepeatStepConfig;
    private SchemaTableTree untilTraversalRootSchemaTableTree;

    private final PGRoutingConfig pgRoutingConfig;

    //NON FINAL properties
    //labels are immutable
    private Set<String> labels;
    private Set<String> realLabelsCache;
    private String reducedLabels;
    private String labeledAliasId;

    private boolean hasIdentifierPrimaryKeyInHierarchy;
    private int hashCode = -1;
    private boolean closed;
    //Only root SchemaTableTrees have these maps;
    private AliasMapHolder aliasMapHolder;
    private boolean eagerLoad = false;
    //This is the incoming element id and the traversals start elements index, for SqlgVertexStep.
    private List<Pair<RecordId.ID, Long>> parentIdsAndIndexes;

    //This counter must only ever be used on the root node of the schema table tree
    //It is used to alias the select clauses
    private int rootAliasCounter = 1;
    //This counter is used for the within predicate when aliasing the temporary table
    private int tmpTableAliasCounter = 1;
    private boolean fakeEmit = false;

    SchemaTableTree(
            SqlgGraph sqlgGraph,
            SchemaTableTree parent,
            SchemaTable schemaTable,
            Direction direction,
            STEP_TYPE stepType,
            int stepDepth,
            int replacedStepDepth,
            boolean idOnly,
            List<HasContainer> hasContainers,
            List<AndOrHasContainer> andOrHasContainers,
            SqlgComparatorHolder sqlgComparatorHolder,
            List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> dbComparators,
            boolean untilFirst,
            boolean emit,
            boolean optionalLeftJoin,
            boolean outerLeftJoin,
            boolean drop,
            boolean localStep,
            boolean localBarrierStep,
            SqlgRangeHolder sqlgRangeHolder,
            List<String> groupBy,
            Pair<String, List<String>> aggregateFunction) {

        this.sqlgGraph = sqlgGraph;
        this.parent = parent;
        this.schemaTable = schemaTable;
        this.direction = direction;
        this.stepType = stepType;
        this.stepDepth = stepDepth;
        this.andOrHasContainers = andOrHasContainers;
        this.replacedStepDepth = replacedStepDepth;
        this.labels = Collections.emptySet();
        this.filteredAllTables = sqlgGraph.getTopology().getAllTables(Topology.SQLG_SCHEMA.equals(schemaTable.getSchema()));
        Pair<ListOrderedSet<String>, String> identifierAndDistributionColumn = setIdentifiersAndDistributionColumn();
        this.identifiers = identifierAndDistributionColumn.getLeft();
        this.distributionColumn = identifierAndDistributionColumn.getRight();
        this.hasIDPrimaryKey = this.identifiers.isEmpty();
        this.idOnly = idOnly;
        this.hasContainers = hasContainers;
        this.sqlgComparatorHolder = sqlgComparatorHolder;
        this.dbComparators = new ArrayList<>(dbComparators);
        this.untilFirst = untilFirst;
        this.emit = emit;
        this.optionalLeftJoin = optionalLeftJoin;
        this.outerLeftJoin = outerLeftJoin;
        this.drop = drop;
        this.localStep = localStep;
        this.localBarrierStep = localBarrierStep;
        this.sqlgRangeHolder = sqlgRangeHolder;
        this.groupBy = groupBy == null ? List.of() : Collections.unmodifiableList(groupBy);
        this.aggregateFunction = aggregateFunction != null ? Pair.of(aggregateFunction.getLeft(), Collections.unmodifiableList(aggregateFunction.getRight())) : null;
        this.recursiveRepeatStepConfig = null;
        this.pgRoutingConfig = null;
    }

    /**
     * This constructor is called for the root SchemaTableTree(s)
     * <p>
     * This is invoked from {@link ReplacedStep} when creating the root {@link SchemaTableTree}s.
     * The hasContainers at this stage contains the {@link TopologyStrategy} from or without hasContainer.
     * After doing the filtering it must be removed from the hasContainers as it must not partake in sql generation.
     */
    public SchemaTableTree(
            SqlgGraph sqlgGraph,
            SchemaTableTree parent,
            SchemaTable schemaTable,
            int stepDepth,
            List<HasContainer> hasContainers,
            List<AndOrHasContainer> andOrHasContainers,
            SqlgComparatorHolder sqlgComparatorHolder,
            List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> dbComparators,
            SqlgRangeHolder sqlgRangeHolder,
            STEP_TYPE stepType,
            boolean emit,
            boolean untilFirst,
            boolean optionalLeftJoin,
            boolean drop,
            int replacedStepDepth,
            Set<String> labels,
            Pair<String, List<String>> aggregateFunction,
            List<String> groupBy,
            boolean idOnly,
            RecursiveRepeatStepConfig recursiveRepeatStepConfig,
            PGRoutingConfig pgRoutingConfig
    ) {
        this.sqlgGraph = sqlgGraph;
        this.parent = parent;
        this.schemaTable = schemaTable;
        this.direction = null;
        this.stepDepth = stepDepth;
        this.hasContainers = new ArrayList<>(hasContainers);
        this.andOrHasContainers = andOrHasContainers;
        this.replacedStepDepth = replacedStepDepth;
        this.sqlgComparatorHolder = sqlgComparatorHolder;
        this.dbComparators = new ArrayList<>(dbComparators);
        this.sqlgRangeHolder = sqlgRangeHolder;
        this.labels = Collections.unmodifiableSet(labels);
        this.stepType = stepType;
        this.emit = emit;
        this.untilFirst = untilFirst;
        this.optionalLeftJoin = optionalLeftJoin;
        this.outerLeftJoin = false;
        this.drop = drop;
        this.aggregateFunction = aggregateFunction != null ? Pair.of(aggregateFunction.getLeft(), Collections.unmodifiableList(aggregateFunction.getRight())) : null;
        this.groupBy = groupBy == null ? List.of() : Collections.unmodifiableList(groupBy);
        this.filteredAllTables = sqlgGraph.getTopology().getAllTables(Topology.SQLG_SCHEMA.equals(schemaTable.getSchema()));
        Pair<ListOrderedSet<String>, String> identifierAndDistributionColumn = setIdentifiersAndDistributionColumn();
        this.identifiers = identifierAndDistributionColumn.getLeft();
        this.distributionColumn = identifierAndDistributionColumn.getRight();
        this.hasIDPrimaryKey = this.identifiers.isEmpty();
        initializeAliasColumnNameMaps();
        this.idOnly = idOnly;
        this.localStep = false;
        this.localBarrierStep = false;
        this.recursiveRepeatStepConfig = recursiveRepeatStepConfig;
        this.pgRoutingConfig = pgRoutingConfig;
    }

    public static void constructDistinctOptionalQueries(SchemaTableTree current, List<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> result) {
        LinkedList<SchemaTableTree> stack = current.constructQueryStackFromLeaf();
        //left joins but not the leave nodes as they are already present in the main sql result set.
        if (current.isOptionalLeftJoin() && (current.getStepDepth() < current.getReplacedStepDepth())) {
            Set<SchemaTableTree> leftyChildren = new HashSet<>(current.children);
            Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>> p = Pair.of(stack, leftyChildren);
            result.add(p);
        }
        for (SchemaTableTree child : current.children) {
            if (child.isVertexStep() && child.getSchemaTable().isVertexTable()) {
                constructDistinctOptionalQueries(child, result);
            } else {
                for (SchemaTableTree vertexChild : child.children) {
                    constructDistinctOptionalQueries(vertexChild, result);
                }
            }
        }
    }

    public static void constructDistinctEmitBeforeQueries(SchemaTableTree current, List<LinkedList<SchemaTableTree>> result) {
        LinkedList<SchemaTableTree> stack = current.constructQueryStackFromLeaf();
        //if its at the full depth it existVertexLabel already been loaded.
        //local step together with emit will createVertexLabel a fake emit. The fake emit will indicate that the incoming traverser must be emitted.
        if (!current.isLocalStep() && current.isEmit() && (current.getStepDepth() < current.getReplacedStepDepth())) {
            result.add(stack);
        }
        if (current.isLocalStep() && current.isEmit()) {
            current.setFakeEmit(true);
        }
        for (SchemaTableTree child : current.children) {
            if (child.isVertexStep() && child.getSchemaTable().isVertexTable()) {
                constructDistinctEmitBeforeQueries(child, result);
            } else {
                for (SchemaTableTree vertexChild : child.children) {
                    constructDistinctEmitBeforeQueries(vertexChild, result);
                }
            }
        }
    }

    private static String constructOuterGroupByClause(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists, boolean dropStep) {
        SchemaTableTree lastSchemaTableTree = subQueryLinkedLists.get(subQueryLinkedLists.size() - 1).getLast();
        StringBuilder result = new StringBuilder();
        if (!lastSchemaTableTree.groupBy.isEmpty() && !lastSchemaTableTree.groupBy.get(0).equals(T.label.getAccessor())) {
            result.append(lastSchemaTableTree.toOuterGroupByClause(sqlgGraph, "a" + subQueryLinkedLists.size()));
            if (!dropStep && subQueryLinkedLists.get(0).getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
                result.append(",\n\t");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
            }
        } else if (lastSchemaTableTree.hasAggregateFunction() && !dropStep && subQueryLinkedLists.get(0).getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
            result.append("\nGROUP BY\n\t");
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
        }
        return result.toString();
    }

    private static String constructOuterOrderByClause(List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        StringBuilder result = new StringBuilder();
        int countOuter = 1;
        // last table list with order as last step wins
        int winningOrder = 0;
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {
            if (!subQueryLinkedList.isEmpty()) {
                SchemaTableTree schemaTableTree = subQueryLinkedList.peekLast();
                if (!schemaTableTree.dbComparators.isEmpty()) {
                    winningOrder = countOuter;
                }
            }
            countOuter++;
        }
        countOuter = 1;
        //construct the order by clause for the comparators
        MutableBoolean mutableOrderBy = new MutableBoolean(false);
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {
            if (!subQueryLinkedList.isEmpty()) {
                SchemaTableTree schemaTableTree = subQueryLinkedList.peekLast();
                if (countOuter == winningOrder) {
                    result.append(schemaTableTree.toOrderByClause(mutableOrderBy, countOuter));
                }
                // support range without order
                result.append(schemaTableTree.toRangeClause(mutableOrderBy));
            }
            countOuter++;
        }
        return result.toString();
    }

    private static String constructSectionedJoin(SqlgGraph sqlgGraph, SchemaTableTree fromSchemaTableTree, SchemaTableTree toSchemaTableTree, int count) {
        Preconditions.checkState(toSchemaTableTree.direction != Direction.BOTH, "Direction may not be BOTH!");
        String rawToLabel;
        if (toSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)) {
            rawToLabel = toSchemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
        } else {
            rawToLabel = toSchemaTableTree.getSchemaTable().getTable();
        }
        String rawFromLabel;
        if (fromSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)) {
            rawFromLabel = fromSchemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
        } else {
            rawFromLabel = fromSchemaTableTree.getSchemaTable().getTable();
        }

        StringBuilder result = new StringBuilder();
        if (fromSchemaTableTree.getSchemaTable().isEdgeTable()) {
            if (toSchemaTableTree.isEdgeVertexStep()) {
                if (toSchemaTableTree.hasIDPrimaryKey) {
                    result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + (toSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                    result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasId()));
                } else {
                    ListOrderedSet<String> identifiers = toSchemaTableTree.getIdentifiers();
                    int i = 1;
                    for (String identifier : identifiers) {
                        if (toSchemaTableTree.isDistributed() && toSchemaTableTree.distributionColumn.equals(identifier)) {
                            result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    fromSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                            identifier));
                            result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
                        } else {
                            result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    fromSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + "." + identifier + (toSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                            result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
                        }
                        if (i++ < identifiers.size()) {
                            result.append(" AND ");
                        }
                    }
                }
            } else {
                if (toSchemaTableTree.direction == Direction.OUT) {
                    if (toSchemaTableTree.hasIDPrimaryKey) {
                        result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                fromSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                        toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + Topology.IN_VERTEX_COLUMN_END));
                        result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasId()));
                    } else {
                        ListOrderedSet<String> identifiers = toSchemaTableTree.getIdentifiers();
                        int i = 1;
                        for (String identifier : identifiers) {
                            if (toSchemaTableTree.isDistributed() && toSchemaTableTree.distributionColumn.equals(identifier)) {
                                result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                        fromSchemaTableTree.getSchemaTable().getSchema() + "." +
                                                fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                                identifier));
                                result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
                            } else {
                                result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                        fromSchemaTableTree.getSchemaTable().getSchema() + "." +
                                                fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                                toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                                result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
                            }
                            if (i++ < identifiers.size()) {
                                result.append(" AND ");
                            }
                        }
                    }
                } else {
                    if (toSchemaTableTree.hasIDPrimaryKey) {
                        result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                        toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + Topology.OUT_VERTEX_COLUMN_END));
                        result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasId()));
                    } else {
                        ListOrderedSet<String> identifiers = toSchemaTableTree.getIdentifiers();
                        int i = 1;
                        for (String identifier : identifiers) {
                            if (toSchemaTableTree.isDistributed() && toSchemaTableTree.distributionColumn.equals(identifier)) {
                                result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                        fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + identifier));
                                result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
                            } else {
                                result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                        fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                                toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                                result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
                            }
                            if (i++ < identifiers.size()) {
                                result.append(" AND ");
                            }
                        }
                    }
                }
            }
        } else {
            if (fromSchemaTableTree.hasIDPrimaryKey) {
                result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + Topology.ID));
                result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.mappedAliasVertexForeignKeyColumnEnd(fromSchemaTableTree, toSchemaTableTree.direction, rawFromLabel)));
            } else {
                ListOrderedSet<String> identifiers = fromSchemaTableTree.getIdentifiers();
                int i = 1;
                for (String identifier : identifiers) {
                    result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                            fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + identifier));
                    result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                            toSchemaTableTree.mappedAliasVertexForeignKeyColumnEnd(fromSchemaTableTree, toSchemaTableTree.direction, rawFromLabel, identifier)));
                    if (i++ < identifiers.size()) {
                        result.append(" AND ");
                    }
                }
            }
        }
        return result.toString();
    }

    public static List<LinkedList<SchemaTableTree>> splitIntoSubStacks(LinkedList<SchemaTableTree> distinctQueryStack) {
        List<LinkedList<SchemaTableTree>> result = new ArrayList<>();
        LinkedList<SchemaTableTree> subList = new LinkedList<>();
        result.add(subList);
        Set<SchemaTable> alreadyVisited = new HashSet<>();
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (!alreadyVisited.contains(schemaTableTree.getSchemaTable())) {
                alreadyVisited.add(schemaTableTree.getSchemaTable());
                subList.add(schemaTableTree);
            } else {
                alreadyVisited.clear();
                subList = new LinkedList<>();
                subList.add(schemaTableTree);
                result.add(subList);
                alreadyVisited.add(schemaTableTree.getSchemaTable());
            }
        }
        return result;
    }

    private static SchemaTable getHasContainerSchemaTable(SchemaTableTree schemaTableTree, SchemaTable predicateSchemaTable) {
        SchemaTable hasContainerLabelSchemaTable;
        //Check if we are on a vertex or edge
        if (schemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)) {
            hasContainerLabelSchemaTable = SchemaTable.of(predicateSchemaTable.getSchema(), VERTEX_PREFIX + predicateSchemaTable.getTable());
        } else {
            hasContainerLabelSchemaTable = SchemaTable.of(predicateSchemaTable.getSchema(), EDGE_PREFIX + predicateSchemaTable.getTable());
        }
        return hasContainerLabelSchemaTable;
    }

    private static SchemaTable getIDContainerSchemaTable(SchemaTableTree schemaTableTree, Object value) {
        RecordId id;
        if (value instanceof Long) {
            return schemaTableTree.getSchemaTable();
        } else if (!(value instanceof RecordId)) {
            id = RecordId.from(String.valueOf(value));
        } else {
            id = (RecordId) value;
        }
        return getHasContainerSchemaTable(schemaTableTree, id.getSchemaTable());

    }

    /**
     * verify the "has" containers we have are valid with the schema table tree given
     *
     * @param schemaTableTree The schemaTableTree to invalidate
     * @return true if any has container does NOT match, false if everything is fine
     */
    @SuppressWarnings("unchecked")
    private static boolean invalidateByHas(SchemaTableTree schemaTableTree) {
        for (HasContainer hasContainer : schemaTableTree.hasContainers) {
            if (!hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_SQLG_SCHEMA) && !hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_GLOBAL_UNIQUE_INDEX)) {
                Preconditions.checkState(!hasContainer.getKey().equals(label.getAccessor()), "label hasContainers should have been removed by now.");
                if (hasContainer.getKey().equals(T.id.getAccessor())) {
                    if (hasContainer.getBiPredicate().equals(Compare.eq)) {
                        Object value = hasContainer.getValue();
                        SchemaTable hasContainerLabelSchemaTable = getIDContainerSchemaTable(schemaTableTree, value);
                        if (!hasContainerLabelSchemaTable.equals(schemaTableTree.getSchemaTable())) {
                            return true;
                        }
                    } else if (hasContainer.getBiPredicate().equals(Contains.within)) {
                        Collection<?> c = (Collection<?>) hasContainer.getPredicate().getValue();
                        Iterator<?> it = c.iterator();
                        Collection<Object> ok = new LinkedList<>();
                        while (it.hasNext()) {
                            Object value = it.next();
                            SchemaTable hasContainerLabelSchemaTable = getIDContainerSchemaTable(schemaTableTree, value);
                            if (hasContainerLabelSchemaTable.equals(schemaTableTree.getSchemaTable())) {
                                ok.add(value);
                            }
                        }
                        if (ok.isEmpty()) {
                            return true;
                        }
                        ((P<Collection<Object>>) (hasContainer.getPredicate())).setValue(ok);
                    }
                } else {
                    if (hasContainer.getBiPredicate() instanceof FullText && ((FullText) hasContainer.getBiPredicate()).getQuery() != null) {
                        return false;
                    }
                    //check if the hasContainer is for a property that exists, if not remove this node from the query tree
                    if (!schemaTableTree.getFilteredAllTables().get(schemaTableTree.getSchemaTable().toString()).containsKey(hasContainer.getKey())) {
                        if (!Existence.NULL.equals(hasContainer.getBiPredicate())) {
                            return true;
                        }
                    }
                    //Check if it is a Contains.within with a empty list of values
                    if (hasEmptyWithin(hasContainer)) {
                        return true;
                    }
                }
            } else {
                throw new IllegalStateException();
            }
        }
        return false;
    }

    private static boolean invalidateByRestrictedProperty(SchemaTableTree schemaTableTree) {
        if (schemaTableTree.idOnly) {
            return false;
        } else if (schemaTableTree.getRestrictedProperties().isEmpty()) {
            return false;
        } else {
            for (String restrictedProperty : schemaTableTree.getRestrictedProperties()) {
                if (schemaTableTree.getFilteredAllTables().get(schemaTableTree.getSchemaTable().toString()).containsKey(restrictedProperty)) {
                    return false;
                }
            }
            return true;
        }
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private static boolean hasEmptyWithin(HasContainer hasContainer) {
        if (hasContainer.getBiPredicate() == Contains.within) {
            return ((Collection<?>) hasContainer.getPredicate().getValue()).isEmpty();
        } else {
            return false;
        }
    }

    public void loadEager() {
        this.eagerLoad = true;
    }

    public void addLabel(String label) {
        Set<String> newLabels = new HashSet<>(getLabels());
        newLabels.add(label);
        this.labels = Collections.unmodifiableSet(newLabels);
    }

    private void clearTempFakeLabels() {
        this.realLabelsCache = null;
        Set<String> toRemove = new HashSet<>();
        for (String label : this.labels) {
            if (label.endsWith(BaseStrategy.SQLG_PATH_TEMP_FAKE_LABEL)) {
                toRemove.add(label);
            }
        }
        Set<String> newLabels = new HashSet<>(getLabels());
        newLabels.removeAll(toRemove);
        this.labels = Collections.unmodifiableSet(newLabels);
    }

    private Pair<ListOrderedSet<String>, String> setIdentifiersAndDistributionColumn() {
        ListOrderedSet<String> _identifiers;
        String _distributionColumn;
        Supplier<IllegalStateException> illegalStateExceptionSupplier = () -> new IllegalStateException(String.format("Label '%s' must be present.", this.schemaTable.toString()));
        if (this.schemaTable.isVertexTable()) {
            VertexLabel vertexLabel = this.sqlgGraph.getTopology().getVertexLabel(
                    this.schemaTable.withOutPrefix().getSchema(),
                    this.schemaTable.withOutPrefix().getTable()
            ).orElseThrow(illegalStateExceptionSupplier);
//            this.identifiers = vertexLabel.getIdentifiers();
            _identifiers = vertexLabel.getIdentifiers();
            if (vertexLabel.isDistributed()) {
//                this.distributionColumn = vertexLabel.getDistributionPropertyColumn().getName();
                _distributionColumn = vertexLabel.getDistributionPropertyColumn().getName();
            } else {
//                this.distributionColumn = null;
                _distributionColumn = null;
            }
        } else {
            EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getEdgeLabel(
                    this.schemaTable.withOutPrefix().getSchema(),
                    this.schemaTable.withOutPrefix().getTable()
            ).orElseThrow(illegalStateExceptionSupplier);
//            this.identifiers = edgeLabel.getIdentifiers();
            _identifiers = edgeLabel.getIdentifiers();
            if (edgeLabel.isDistributed()) {
//                this.distributionColumn = edgeLabel.getDistributionPropertyColumn().getName();
                _distributionColumn = edgeLabel.getDistributionPropertyColumn().getName();
            } else {
//                this.distributionColumn = null;
                _distributionColumn = null;
            }
        }
        return Pair.of(_identifiers, _distributionColumn);
    }

    SchemaTableTree addChild(
            SchemaTable schemaTable,
            Direction direction,
            ReplacedStep<?, ?> replacedStep,
            Set<String> labels) {

        return addChild(
                schemaTable,
                direction,
                Vertex.class,
                replacedStep.getHasContainers(),
                replacedStep.getAndOrHasContainers(),
                replacedStep.getSqlgComparatorHolder(),
                replacedStep.getSqlgComparatorHolder().getComparators(),
                replacedStep.getSqlgRangeHolder(),
                replacedStep.getRestrictedProperties(),
                replacedStep.getAggregateFunction(),
                replacedStep.getGroupBy(),
                replacedStep.getDepth(),
                true,
                replacedStep.isEmit(),
                replacedStep.isUntilFirst(),
                replacedStep.isLeftJoin(),
                replacedStep.isOuterLeftJoin(),
                replacedStep.isDrop(),
                replacedStep.isIdOnly(),
                labels);
    }

    SchemaTableTree addChild(
            SchemaTable schemaTable,
            Direction direction,
            Class<? extends Element> elementClass,
            ReplacedStep<?, ?> replacedStep,
            Set<String> labels) {

        Preconditions.checkState(replacedStep.getStep() instanceof VertexStep, "addChild can only be called for a VertexStep, found %s", replacedStep.getStep().getClass().getSimpleName());
        //Do not emit the edge schema table for a vertex step.
        boolean emit;
        if (elementClass.isAssignableFrom(Vertex.class)) {
            emit = schemaTable.isVertexTable() && replacedStep.isEmit();
        } else if (elementClass.isAssignableFrom(Edge.class)) {
            emit = schemaTable.isEdgeTable() && replacedStep.isEmit();
        } else {
            throw new IllegalStateException(String.format("BUG: Expected %s, instead found %s", "Edge or Vertex", elementClass.getSimpleName()));
        }

        return addChild(
                schemaTable,
                direction,
                elementClass,
                replacedStep.getHasContainers(),
                replacedStep.getAndOrHasContainers(),
                replacedStep.getSqlgComparatorHolder(),
                replacedStep.getSqlgComparatorHolder().getComparators(),
                replacedStep.getSqlgRangeHolder(),
                replacedStep.getRestrictedProperties(),
                replacedStep.getAggregateFunction(),
                replacedStep.getGroupBy(),
                replacedStep.getDepth(),
                false,
                emit,
                replacedStep.isUntilFirst(),
                replacedStep.isLeftJoin(),
                replacedStep.isOuterLeftJoin(),
                replacedStep.isDrop(),
                replacedStep.isIdOnly(),
                labels);
    }

    private SchemaTableTree addChild(
            SchemaTable schemaTable,
            Direction direction,
            Class<? extends Element> elementClass,
            List<HasContainer> hasContainers,
            List<AndOrHasContainer> andOrHasContainers,
            SqlgComparatorHolder sqlgComparatorHolder,
            List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> dbComparators,
            SqlgRangeHolder sqlgRangeHolder,
            Set<String> restrictedProperties,
            Pair<String, List<String>> aggregateFunction,
            List<String> groupBy,
            int stepDepth,
            boolean isEdgeVertexStep,
            boolean emit,
            boolean untilFirst,
            boolean leftJoin,
            boolean outerLeftJoin,
            boolean drop,
            boolean idOnly,
            Set<String> labels) {

        SchemaTableTree schemaTableTree;
        if ((elementClass.isAssignableFrom(Edge.class) && schemaTable.getTable().startsWith(EDGE_PREFIX)) ||
                (elementClass.isAssignableFrom(Vertex.class) && schemaTable.getTable().startsWith(VERTEX_PREFIX))) {

            schemaTableTree = new SchemaTableTree(
                    this.sqlgGraph,
                    this,
                    schemaTable,
                    direction,
                    isEdgeVertexStep ? STEP_TYPE.EDGE_VERTEX_STEP : STEP_TYPE.VERTEX_STEP,
                    stepDepth,
                    this.replacedStepDepth,
                    idOnly,
                    new ArrayList<>(hasContainers),
                    andOrHasContainers,
                    sqlgComparatorHolder,
                    new ArrayList<>(dbComparators),
                    untilFirst,
                    emit,
                    leftJoin,
                    outerLeftJoin,
                    drop,
                    false,
                    false,
                    sqlgRangeHolder,
                    groupBy,
                    aggregateFunction
            );

        } else {

            schemaTableTree = new SchemaTableTree(
                    this.sqlgGraph,
                    this,
                    schemaTable,
                    direction,
                    isEdgeVertexStep ? STEP_TYPE.EDGE_VERTEX_STEP : STEP_TYPE.VERTEX_STEP,
                    stepDepth,
                    this.replacedStepDepth,
                    idOnly,
                    new ArrayList<>(),
                    new ArrayList<>(),
                    sqlgComparatorHolder,
                    new ArrayList<>(),
                    untilFirst,
                    emit,
                    leftJoin,
                    outerLeftJoin,
                    drop,
                    false,
                    false,
                    null,
                    groupBy,
                    aggregateFunction
            );

        }
        this.children.add(schemaTableTree);
        schemaTableTree.labels = Collections.unmodifiableSet(labels);
        schemaTableTree.getRestrictedProperties().addAll(restrictedProperties);
        return schemaTableTree;
    }

    private Map<String, Map<String, PropertyDefinition>> getFilteredAllTables() {
        return getRoot().filteredAllTables;
    }

    public Map<String, PropertyDefinition> getPropertyDefinitions() {
        return this.getFilteredAllTables().get(this.schemaTable.toString());
    }

    void initializeAliasColumnNameMaps() {
        this.getRoot().aliasMapHolder = new AliasMapHolder();
    }

    public AliasMapHolder getAliasMapHolder() {
        return getRoot().aliasMapHolder;
    }

    public void setAliasMapHolder(AliasMapHolder aliasMapHolder) {
        this.getRoot().aliasMapHolder = aliasMapHolder;
    }

    public Map<String, String> getColumnNameAliasMap() {
        return this.getRoot().aliasMapHolder.getColumnNameAliasMap();
    }

    public Map<String, String> getAliasColumnNameMap() {
        return this.getRoot().aliasMapHolder.getAliasColumnNameMap();
    }

    public Set<String> getAllIdentifiers() {
        Set<String> result = new HashSet<>();
        internalAllIdentifiers(result);
        return result;
    }

    private void internalAllIdentifiers(Set<String> allIdentifiers) {
        allIdentifiers.addAll(this.identifiers);
        for (SchemaTableTree child : this.children) {
            child.internalAllIdentifiers(allIdentifiers);
        }
    }

    private boolean hasParent() {
        return this.parent != null;
    }

    private SchemaTableTree getRoot() {
        return walkUp(this);
    }

    private SchemaTableTree walkUp(SchemaTableTree schemaTableTree) {
        if (schemaTableTree.hasParent()) {
            return schemaTableTree.walkUp(schemaTableTree.getParent());
        } else {
            return schemaTableTree;
        }
    }

    public boolean isEmit() {
        return this.emit;
    }

    public boolean isOptionalLeftJoin() {
        return this.optionalLeftJoin;
    }

    public boolean isOuterLeftJoin() {
        return outerLeftJoin;
    }

    public void resetColumnAliasMaps() {
        this.getRoot().aliasMapHolder.clear();
        this.rootAliasCounter = 1;
    }

    public SchemaTable getSchemaTable() {
        return schemaTable;
    }

    public String constructSql(LinkedList<SchemaTableTree> distinctQueryStack) {
        Preconditions.checkState(this.parent == null, CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT);
        //If the same element occurs multiple times in the stack then the sql needs to be different.
        //This is because the same element can not be joined on more than once in sql
        //The way to overcome this is to break up the path in select sections with no duplicates and then join them together.

        if (distinctQueryStack.getFirst().isRecursiveQuery()) {
            return constructRecursiveQuery(distinctQueryStack);
        } else if (distinctQueryStack.getFirst().isPGRoutingQuery()) {
            return constructPGRoutingQuery(distinctQueryStack);
        } else if (duplicatesInStack(distinctQueryStack)) {
            List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(distinctQueryStack);
            return constructDuplicatePathSql(subQueryStacks, Collections.emptySet());
        } else {
            //If there are no duplicates in the path then one select statement will suffice.
            return constructSinglePathSql(
                    false,
                    distinctQueryStack,
                    null,
                    null,
                    Collections.emptySet(),
                    false
            );
        }
    }

    public String constructSqlForOptional(LinkedList<SchemaTableTree> innerJoinStack, Set<SchemaTableTree> leftJoinOn) {
        Preconditions.checkState(this.parent == null, CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT);
        if (duplicatesInStack(innerJoinStack)) {
            List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(innerJoinStack);
            return constructDuplicatePathSql(subQueryStacks, leftJoinOn);
        } else {
            //If there are no duplicates in the path then one select statement will suffice.
            return constructSinglePathSql(
                    false,
                    innerJoinStack,
                    null,
                    null,
                    leftJoinOn,
                    false
            );
        }
    }

    public List<SqlgSqlExecutor.DropQuery> constructDropSql(LinkedList<SchemaTableTree> distinctQueryStack) {
        Preconditions.checkState(this.parent == null, CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT);
        Preconditions.checkState(distinctQueryStack.getLast().drop);
        Preconditions.checkState(!duplicatesInStack(distinctQueryStack));

        if (distinctQueryStack.size() == 1 &&
                distinctQueryStack.getFirst().getHasContainers().isEmpty() &&
                distinctQueryStack.getFirst().getAndOrHasContainers().isEmpty() &&
                (
                        (this.sqlgGraph.getSqlDialect().supportsTruncateMultipleTablesTogether() && hasOnlyOneInOutEdgeLabel(distinctQueryStack.getFirst().getSchemaTable())) ||
                                (!this.sqlgGraph.getSqlDialect().supportsTruncateMultipleTablesTogether() && hasNoEdgeLabels(distinctQueryStack.getFirst().getSchemaTable()))
                )) {
            //truncate logic.
            SchemaTableTree schemaTableTree = distinctQueryStack.getFirst();
            return this.sqlgGraph.getSqlDialect().sqlTruncate(this.sqlgGraph, schemaTableTree.getSchemaTable());
        } else {
            String leafNodeToDelete = constructSinglePathSql(
                    false,
                    distinctQueryStack,
                    null,
                    null,
                    Collections.emptySet(),
                    true
            );

            Optional<String> edgesToDelete = Optional.empty();
            if (distinctQueryStack.size() > 1 && distinctQueryStack.getLast().getSchemaTable().isVertexTable()) {
                Set<SchemaTableTree> leftJoin = new HashSet<>();
                leftJoin.add(distinctQueryStack.getLast());
                LinkedList<SchemaTableTree> edgeSchemaTableTrees = new LinkedList<>(distinctQueryStack);
                edgeSchemaTableTrees.removeLast();
                edgesToDelete = Optional.of(constructSinglePathSql(
                        false,
                        edgeSchemaTableTrees,
                        null,
                        null,
                        leftJoin,
                        true
                ));
            }
            return this.sqlgGraph.getSqlDialect().drop(this.sqlgGraph, leafNodeToDelete, edgesToDelete.orElse(null), distinctQueryStack);
        }

    }

    public List<LinkedList<SchemaTableTree>> constructDistinctQueries() {
        Preconditions.checkState(this.parent == null, "constructDistinctQueries may only be called on the root object");
        List<LinkedList<SchemaTableTree>> result = new ArrayList<>();
        for (SchemaTableTree leafNode : this.leafNodes) {
            if (leafNode.getStepDepth() == this.replacedStepDepth) {
                result.add(leafNode.constructQueryStackFromLeaf());
            }
        }
        Preconditions.checkState(result.stream().noneMatch(schemaTableTrees -> schemaTableTrees.get(0).getParent() != null), "Expected root SchemaTableTree for the first SchemaTableTree in the LinkedList");
        return result;
    }

    /**
     * Construct a sql statement for one original path to a leaf node.
     * As the path contains the same label more than once it's been split into a List of Stacks.
     */
    private String constructDuplicatePathSql(List<LinkedList<SchemaTableTree>> subQueryLinkedLists, Set<SchemaTableTree> leftJoinOn) {
        StringBuilder singlePathSql = new StringBuilder("\nFROM (");
        int count = 1;
        SchemaTableTree lastOfPrevious = null;
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {
            SchemaTableTree firstOfNext = null;
            boolean last = count == subQueryLinkedLists.size();
            if (!last) {
                //this is to get the next SchemaTable to join to
                LinkedList<SchemaTableTree> nextList = subQueryLinkedLists.get(count);
                firstOfNext = nextList.getFirst();
            }
            SchemaTableTree firstSchemaTableTree = subQueryLinkedList.getFirst();

            String sql;
            if (last) {
                //only the last step must have dropStep as true. As only the outer select needs only an ID in the select
                sql = constructSinglePathSql(
                        true,
                        subQueryLinkedList,
                        lastOfPrevious,
                        null,
                        leftJoinOn,
                        false
                );
            } else {
                sql = constructSinglePathSql(
                        true,
                        subQueryLinkedList,
                        lastOfPrevious,
                        firstOfNext,
                        Collections.emptySet(),
                        false
                );
            }
            singlePathSql.append(sql);
            if (count == 1) {
                singlePathSql.append("\n) a").append(count++).append(" INNER JOIN (");
            } else {
                //join the last with the first
                singlePathSql.append("\n) a").append(count).append(" ON ");
                singlePathSql.append(constructSectionedJoin(sqlgGraph, lastOfPrevious, firstSchemaTableTree, count));
                if (count++ < subQueryLinkedLists.size()) {
                    singlePathSql.append(" INNER JOIN (");
                }
            }
            lastOfPrevious = subQueryLinkedList.getLast();
        }
        singlePathSql.append(constructOuterGroupByClause(sqlgGraph, subQueryLinkedLists, isDrop()));
        singlePathSql.append(constructOuterOrderByClause(subQueryLinkedLists));
        String result = "SELECT\n\t" + constructOuterFromClause(subQueryLinkedLists);
        return result + singlePathSql;
    }

    private String constructOuterFromClause(List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        StringBuilder result = new StringBuilder();
        int countOuter = 1;
        boolean first = true;
        int i = 1;
        int columnStartIndex = 1;
        Preconditions.checkState(getRoot() == this);
        boolean stackContainsAggregate = this.getAliasMapHolder().getColumnListStack().get(this.getAliasMapHolder().getColumnListStack().size() - 1).isContainsAggregate();
        if (stackContainsAggregate) {
            for (ColumnList columnList : this.getAliasMapHolder().getColumnListStack()) {
                columnList.removeColumns(subQueryLinkedLists.get(subQueryLinkedLists.size() - 1).getLast());
            }
        }

        for (ColumnList columnList : this.getAliasMapHolder().getColumnListStack()) {


            if (first && subQueryLinkedLists.get(0).getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
                result.append("a1.").append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index")).append(" as ")
                        .append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"))
                        .append(",\n\r");

                columnStartIndex++;
            }
            first = false;
            String from = columnList.toOuterFromString("a" + countOuter++, stackContainsAggregate);
            result.append(from);
            if (i++ < this.getAliasMapHolder().getColumnListStack().size() && !from.isEmpty()) {
                result.append(", ");
            }
            columnStartIndex = columnList.reindexColumnsExcludeForeignKey(columnStartIndex, stackContainsAggregate);
        }
        return result.toString();
    }

    private String constructPGRoutingQuery(LinkedList<SchemaTableTree> distinctQueryStack) {
        Preconditions.checkState(distinctQueryStack.size() <= 2);
        SchemaTableTree schemaTableTree = distinctQueryStack.getFirst();
        Preconditions.checkNotNull(schemaTableTree.pgRoutingConfig);
        Preconditions.checkState(this == schemaTableTree);

        LinkedList<SchemaTableTree> startDistinctQueryStack = new LinkedList<>();
        startDistinctQueryStack.add(distinctQueryStack.getFirst());
        String startSql = constructSinglePathSql(
                false,
                startDistinctQueryStack,
                null,
                null,
                Collections.emptySet(),
                false
        );

        resetColumnAliasMaps();
        String ignore = constructSinglePathSql(
                false,
                distinctQueryStack,
                null,
                null,
                Collections.emptySet(),
                false
        );

        PGRoutingConfig pgRoutingConfig = schemaTableTree.pgRoutingConfig;
        VertexLabel vertexLabel = pgRoutingConfig.vertexLabel();
        SchemaTable vertexLabelSchemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), Topology.VERTEX_PREFIX + vertexLabel.getName());
        EdgeLabel edgeLabel = pgRoutingConfig.edgeLabel();
        SchemaTable edgeLabelSchemaTable = SchemaTable.of(edgeLabel.getSchema().getName(), Topology.EDGE_PREFIX + edgeLabel.getName());

        List<String> vertexColumns = new ArrayList<>();
        List<String> edgeColumns = new ArrayList<>();
        List<ColumnList> columnLists = getRootColumnListStack();
        for (ColumnList columnList : columnLists) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(0, edgeLabelSchemaTable);
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID() && !column.isForeignKey()) {
                    edgeColumns.add(column.getColumn());
                }
            }
            columns = columnList.getFor(1, vertexLabelSchemaTable);
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID()) {
                    vertexColumns.add(column.getColumn());
                }
            }
        }

        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + vertexLabelSchemaTable.withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + vertexLabelSchemaTable.withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexColumnsToAdd = vertexColumns.stream().map(a -> "b." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String edgeColumnsToAdd = edgeColumns.stream().map(a -> "c." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");

        String sql = """
                WITH a AS (
                    SELECT * FROM pgr_dijkstra(
                    'SELECT a."alias3" as id, a."alias2" as source, a."alias1" as target, a."alias4" as cost FROM (
                        %s
                    ) a', %d, %d, %b)
                ), b AS (
                    SELECT * from %s
                ), c AS (
                    SELECT * from %s
                )
                SELECT c."ID" as edge_id {edgeColumns}, c.{inForeignKey}, c.{outForeignKey}, b."ID" as vertex_id {vertexColumns} FROM
                    a JOIN
                    b ON a.node = b."ID" LEFT JOIN
                    c ON a.edge = c."ID"
                """
                .formatted(
                        startSql,
                        pgRoutingConfig.start_vid(),
                        pgRoutingConfig.end_vid(),
                        pgRoutingConfig.directed(),
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabelSchemaTable.getSchema()) + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabelSchemaTable.getTable()),
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeLabelSchemaTable.getSchema()) + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeLabelSchemaTable.getTable())
                )
                .replace("{edgeColumns}", edgeColumnsToAdd)
                .replace("{vertexColumns}", vertexColumnsToAdd)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey);
        return sql;
    }

    private String constructRecursiveQuery(LinkedList<SchemaTableTree> distinctQueryStack) {
        Preconditions.checkState(distinctQueryStack.size() <= 2);
        SchemaTableTree schemaTableTree = distinctQueryStack.getFirst();
        Preconditions.checkNotNull(schemaTableTree.recursiveRepeatStepConfig);
        Preconditions.checkState(this == schemaTableTree);

        List<LinkedList<SchemaTableTree>> untilDistinctQueryStacks = this.untilTraversalRootSchemaTableTree.constructDistinctQueries();
        Preconditions.checkState(untilDistinctQueryStacks.size() == 1);
        LinkedList<SchemaTableTree> untilDistinctQueryStack = untilDistinctQueryStacks.get(0);
        Preconditions.checkState(untilDistinctQueryStack.size() <= 2);
        SchemaTableTree untilSchemaTableTree;
        boolean includeEdge = untilDistinctQueryStack.size() > 1;
        if (!includeEdge) {
            untilSchemaTableTree = untilDistinctQueryStack.get(0);
        } else {
            //includeEdge
            untilSchemaTableTree = untilDistinctQueryStack.get(1);
        }
        String optionalUntilWhereClause = untilSchemaTableTree.toWhereClause(new MutableBoolean(false));

        LinkedList<SchemaTableTree> startDistinctQueryStack = new LinkedList<>();
        startDistinctQueryStack.add(distinctQueryStack.getFirst());

        String startSql = constructSinglePathSql(
                false,
                startDistinctQueryStack,
                null,
                null,
                Collections.emptySet(),
                false
        );

        ColumnList columnList = this.getAliasMapHolder().getColumnListStack().get(0);

        String select = columnList.toSelectString(false);

        resetColumnAliasMaps();

        //we need to do this to create the ColumnList
        //if includeEdge = true, then the columnList thinks there are 2 additional columns for the foreignKeys,
        //This maps in a bad way to the 2 additional columns for the 'path' and 'type'
        String ignore = constructSinglePathSql(
                false,
                distinctQueryStack,
                null,
                null,
                Collections.emptySet(),
                false,
                includeEdge ? 1 : 2
        );
        String sql;
        //includes the edge
        if (distinctQueryStack.size() == 2) {
            if (this.recursiveRepeatStepConfig.hasNotStepForPathTraversal()) {
                sql = switch (this.recursiveRepeatStepConfig.direction()) {
                    case OUT ->
                            constructRecursiveIncludeEdgeOutQuery(startSql, distinctQueryStack, optionalUntilWhereClause);
                    case IN ->
                            constructRecursiveIncludeEdgeInQuery(startSql, distinctQueryStack, optionalUntilWhereClause);
                    case BOTH ->
                            constructRecursiveIncludeEdgeBothQuery(startSql, distinctQueryStack, optionalUntilWhereClause);
                };
            } else {
                sql = switch (this.recursiveRepeatStepConfig.direction()) {
                    case OUT ->
                            constructRecursiveIncludeEdgeOutQueryWithoutNotStep(startSql, distinctQueryStack, optionalUntilWhereClause);
                    case IN ->
                            constructRecursiveIncludeEdgeInQueryWithoutNotStep(startSql, distinctQueryStack, optionalUntilWhereClause);
                    case BOTH ->
                            constructRecursiveIncludeEdgeBothQueryWithoutNotStep(startSql, distinctQueryStack, optionalUntilWhereClause);
                };
            }
        } else {
            if (this.recursiveRepeatStepConfig.hasNotStepForPathTraversal()) {
                sql = switch (this.recursiveRepeatStepConfig.direction()) {
                    case OUT ->
                            constructRecursiveOutQuery(startSql, select, this.recursiveRepeatStepConfig.edge(), optionalUntilWhereClause);
                    case IN ->
                            constructRecursiveInQuery(startSql, select, this.recursiveRepeatStepConfig.edge(), optionalUntilWhereClause);
                    case BOTH ->
                            constructRecursiveBothQuery(startSql, select, this.recursiveRepeatStepConfig.edge(), optionalUntilWhereClause);
                };
            } else {
                sql = switch (this.recursiveRepeatStepConfig.direction()) {
                    case OUT ->
                            constructRecursiveOutQueryWithoutNotStep(startSql, select, this.recursiveRepeatStepConfig.edge(), optionalUntilWhereClause);
                    case IN ->
                            constructRecursiveInQueryWithoutNotStep(startSql, select, this.recursiveRepeatStepConfig.edge(), optionalUntilWhereClause);
                    default ->
                            constructRecursiveBothQueryWithoutNotStep(startSql, select, this.recursiveRepeatStepConfig.edge(), optionalUntilWhereClause);
                };
            }
        }
        //merge the main distinctQueryStack and until distinctQueryStack
        schemaTableTree.getAndOrHasContainers().addAll(untilSchemaTableTree.getAndOrHasContainers());
        if (!this.recursiveRepeatStepConfig.hasNotStepForPathTraversal()) {
            //Do it again for the second generated WHERE clause
            schemaTableTree.getAndOrHasContainers().addAll(untilSchemaTableTree.getAndOrHasContainers());
        }
        return sql;
    }

    private String constructSinglePathSql(
            boolean partOfDuplicateQuery,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree lastOfPrevious,
            SchemaTableTree firstOfNextStack,
            Set<SchemaTableTree> leftJoinOn,
            boolean dropStep) {

        return constructSinglePathSql(
                partOfDuplicateQuery,
                distinctQueryStack,
                lastOfPrevious,
                firstOfNextStack,
                leftJoinOn,
                dropStep,
                1
        );

    }

    /**
     * Constructs a sql select statement from the SchemaTableTree call stack.
     * The SchemaTableTree is not used as a tree. It is used only as as SchemaTable with a direction.
     * first and last is needed to facilitate generating the from statement.
     * If both first and last is true then the gremlin does not contain duplicate labels in its path and
     * can be executed in one sql statement.
     * If first and last is not equal then the sql will join across many select statements.
     * The previous select needs to join onto the subsequent select. For this the from needs to select the appropriate
     * field for the join.
     */
    private String constructSinglePathSql(
            boolean partOfDuplicateQuery,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree lastOfPrevious,
            SchemaTableTree firstOfNextStack,
            Set<SchemaTableTree> leftJoinOn,
            boolean dropStep,
            int _startIndexColumns) {

        Preconditions.checkState(this.parent == null, "constructSelectSinglePathSql may only be called on the root SchemaTableTree");
        distinctQueryStack.forEach(SchemaTableTree::clearTempFakeLabels);

        //calculate restrictions on properties once and for all
        calculatePropertyRestrictions();
        for (SchemaTableTree stt : distinctQueryStack) {
            if (stt != this) {
                stt.calculatePropertyRestrictions();
            }
        }
        //columnList holds the columns per sub query.
        ColumnList currentColumnList = new ColumnList(this.sqlgGraph, dropStep, this.getIdentifiers(), this.getFilteredAllTables());

        Preconditions.checkState(getRoot() == this);
        this.getAliasMapHolder().getColumnListStack().add(currentColumnList);

        int startIndexColumns = _startIndexColumns;

        StringBuilder singlePathSql = new StringBuilder("SELECT\n\t");
        SchemaTableTree firstSchemaTableTree = distinctQueryStack.getFirst();
        SchemaTable firstSchemaTable = firstSchemaTableTree.getSchemaTable();

        //The SqlgVertexStep's incoming/parent element index and ids
        //dropStep must not have the index as it uses 'delete from where in (select...)' or 'WITH (SELECT) DELETE...'
        //the first column in the select must be the ID.
        //As it's a DELETE there is no need for the 'index' to order on.
        if (!dropStep && lastOfPrevious == null && distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
            //if there is only 1 incoming start/traverser we use a where clause as its faster.
            //ms sql server does not support alias's in the group by clause.
            //using the join avoids that constraint so we always join to the value expression for ms sql servers.
            if (!this.sqlgGraph.getSqlDialect().isMssqlServer() && this.parentIdsAndIndexes.size() == 1) {
                singlePathSql.append(this.parentIdsAndIndexes.get(0).getRight());
                singlePathSql.append(" as ");
                singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
            } else if (this.sqlgGraph.getSqlDialect().supportsValuesExpression()) {
                //Hard-coding here for H2
                if (this.sqlgGraph.getSqlDialect().supportsFullValueExpression()) {
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                } else {
                    if (firstSchemaTableTree.hasIDPrimaryKey) {
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("C2"));
                    } else {
                        singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("C" + (firstSchemaTableTree.getIdentifiers().size() + 1)));
                    }
                }
                singlePathSql.append(" as ");
                singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
            } else {
                singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                singlePathSql.append(" as ");
                singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
            }
            singlePathSql.append(",\n\t");
            //increment the ColumnList's index to take the "index" field into account.
            startIndexColumns++;
        }

        singlePathSql.append(
                constructSelectClause(
                        dropStep,
                        currentColumnList,
                        distinctQueryStack,
                        lastOfPrevious,
                        firstOfNextStack,
                        partOfDuplicateQuery
                )
        );
        singlePathSql.append("\nFROM\n\t");
        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getSchema()));
        singlePathSql.append(".");
        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getTable()));
        SchemaTableTree previous = firstSchemaTableTree;
        boolean skipFirst = true;
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (skipFirst) {
                skipFirst = false;
                continue;
            }
            singlePathSql.append(constructJoinBetweenSchemaTables(previous, schemaTableTree, false));
            previous = schemaTableTree;
        }

        SchemaTableTree previousLeftJoinSchemaTableTree = null;
        for (SchemaTableTree schemaTableTree : leftJoinOn) {
            if (previousLeftJoinSchemaTableTree == null || !previousLeftJoinSchemaTableTree.getSchemaTable().equals(schemaTableTree.getSchemaTable())) {
                singlePathSql.append(constructJoinBetweenSchemaTables(previous, schemaTableTree, true));
            } else {
                singlePathSql.append(appendToJoinBetweenSchemaTables(previous, schemaTableTree));
            }
            previousLeftJoinSchemaTableTree = schemaTableTree;
        }

        //Check if there is a hasContainer with a P.within more than x.
        //If so add in a join to the temporary table that will hold the values of the P.within predicate.
        //These values are inserted/copy command into a temporary table before joining.
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (this.sqlgGraph.getSqlDialect().supportsBulkWithinOut() && schemaTableTree.hasBulkWithinOrOut()) {
                singlePathSql.append(schemaTableTree.bulkWithJoin());
            }
        }

        MutableBoolean mutableWhere = new MutableBoolean(false);
        MutableBoolean mutableOrderBy = new MutableBoolean(false);

        //lastOfPrevious is null for the first call in the call stack it needs the id parameter in the where clause.
        if (lastOfPrevious == null && distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
            if ((this.sqlgGraph.getSqlDialect().isMssqlServer() || this.parentIdsAndIndexes.size() != 1) &&
                    this.sqlgGraph.getSqlDialect().supportsValuesExpression()) {

                singlePathSql.append(" INNER JOIN\n\t(VALUES");
                int count = 1;
                for (Pair<RecordId.ID, Long> parentIdAndIndex : this.parentIdsAndIndexes) {
                    RecordId.ID id = parentIdAndIndex.getKey();
                    Long index = parentIdAndIndex.getValue();
                    singlePathSql.append("(");
                    if (id.hasSequenceId()) {
                        singlePathSql.append(id.getSequenceId());
                        singlePathSql.append(", ");
                        singlePathSql.append(index);
                    } else {
                        for (Comparable identifierValue : id.getIdentifiers()) {
                            singlePathSql.append("'");
                            singlePathSql.append(identifierValue);
                            singlePathSql.append("'");
                            singlePathSql.append(", ");
                        }
                        singlePathSql.append(index);
                    }
                    singlePathSql.append(")");
                    if (count++ < this.parentIdsAndIndexes.size()) {
                        singlePathSql.append(",");
                    }
                }

                if (this.sqlgGraph.getSqlDialect().supportsFullValueExpression()) {
                    singlePathSql.append(") AS tmp (");
                    if (firstSchemaTableTree.hasIDPrimaryKey) {
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("tmpId"));
                        singlePathSql.append(", ");
                    } else {
                        for (String identifier : firstSchemaTableTree.getIdentifiers()) {
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                            singlePathSql.append(", ");
                        }
                    }
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                    singlePathSql.append(") ON ");

                    if (firstSchemaTableTree.hasIDPrimaryKey) {
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                        singlePathSql.append(".");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                        singlePathSql.append(".");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                        singlePathSql.append(" = tmp.");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("tmpId"));
                    } else {
                        int cnt = 1;
                        for (String identifier : firstSchemaTableTree.getIdentifiers()) {
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                            singlePathSql.append(".");
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                            singlePathSql.append(".");
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                            singlePathSql.append(" = tmp.");
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                            if (cnt++ < firstSchemaTableTree.getIdentifiers().size()) {
                                singlePathSql.append(" AND ");
                            }
                        }
                    }
                } else {
                    //This really is only for H2
                    singlePathSql.append(") ON ");
                    if (firstSchemaTableTree.hasIDPrimaryKey) {
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                        singlePathSql.append(".");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                        singlePathSql.append(".");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                        singlePathSql.append(" = ");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("C1"));
                    } else {
                        int cnt = 1;
                        for (String identifier : firstSchemaTableTree.getIdentifiers()) {
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                            singlePathSql.append(".");
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                            singlePathSql.append(".");
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                            singlePathSql.append(" = ");
                            singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("C" + cnt));
                            if (cnt++ < firstSchemaTableTree.getIdentifiers().size()) {
                                singlePathSql.append(" AND ");
                            }
                        }
                    }
                }
            } else if (this.parentIdsAndIndexes.size() != 1 && !this.sqlgGraph.getSqlDialect().supportsValuesExpression()) {
                //Mariadb supports VALUES expression but not in a useful manner.
                //https://jira.mariadb.org/browse/MDEV-16771
                //Need to use a randomized name here else the temp table gets reused within the same transaction.
                SecureRandom random = new SecureRandom();
                byte[] bytes = new byte[6];
                random.nextBytes(bytes);
                String tmpTableIdentified = Base64.getEncoder().encodeToString(bytes);
                this.sqlgGraph.tx().normalBatchModeOn();
                for (Pair<RecordId.ID, Long> parentIdsAndIndex : this.parentIdsAndIndexes) {
                    if (firstSchemaTableTree.hasIDPrimaryKey) {
                        this.sqlgGraph.addTemporaryVertex(
                                T.label, tmpTableIdentified,
                                "tmpId", parentIdsAndIndex.getLeft().getSequenceId(),
                                "index", parentIdsAndIndex.getRight());
                    } else {
                        List<Object> keyValues = new ArrayList<>();
                        keyValues.add(T.label);
                        keyValues.add(tmpTableIdentified);
                        int count = 0;
                        for (String identifier : firstSchemaTableTree.getIdentifiers()) {
                            keyValues.add(identifier);
                            keyValues.add(parentIdsAndIndex.getLeft().getIdentifiers().get(count++));
                        }
                        keyValues.add("index");
                        keyValues.add(parentIdsAndIndex.getRight());
                        this.sqlgGraph.addTemporaryVertex(keyValues.toArray());
                    }
                }
                this.sqlgGraph.tx().flush();

                singlePathSql.append(" INNER JOIN\n\t");
                singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
                singlePathSql.append(".");
                singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + tmpTableIdentified));
                singlePathSql.append(" as tmp");
                singlePathSql.append(" ON ");

                if (firstSchemaTableTree.hasIDPrimaryKey) {
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                    singlePathSql.append(".");
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                    singlePathSql.append(".");
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                    singlePathSql.append(" = tmp.");
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("tmpId"));
                } else {
                    int cnt = 1;
                    for (String identifier : firstSchemaTableTree.getIdentifiers()) {
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                        singlePathSql.append(".");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                        singlePathSql.append(".");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        singlePathSql.append(" = tmp.");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (cnt++ < firstSchemaTableTree.getIdentifiers().size()) {
                            singlePathSql.append(" AND ");
                        }
                    }
                }
            } else {
                singlePathSql.append("\nWHERE\n\t");
                if (firstSchemaTableTree.hasIDPrimaryKey) {
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                    singlePathSql.append(".");
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                    singlePathSql.append(".");
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                    singlePathSql.append(" = ");
                    singlePathSql.append(this.parentIdsAndIndexes.get(0).getLeft());
                } else {
                    int cnt = 1;
                    for (String identifier : firstSchemaTableTree.getIdentifiers()) {
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                        singlePathSql.append(".");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                        singlePathSql.append(".");
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        singlePathSql.append(" = ");
                        PropertyDefinition propertyDefinition = this.filteredAllTables.get(firstSchemaTable.getSchema() + "." + firstSchemaTable.getTable()).get(identifier);
                        singlePathSql.append(this.sqlgGraph.getSqlDialect().toRDBSStringLiteral(propertyDefinition.propertyType(), this.parentIdsAndIndexes.get(0).getLeft().getIdentifiers().get(cnt - 1)));
                        if (cnt++ < firstSchemaTableTree.getIdentifiers().size()) {
                            singlePathSql.append(" AND ");
                        }
                    }
                }
                mutableWhere.setTrue();
            }
        }

        //construct the where clause for the hasContainers
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            singlePathSql.append(schemaTableTree.toWhereClause(mutableWhere));
        }
        //add in the is null where clause for the optional left joins
        for (SchemaTableTree schemaTableTree : leftJoinOn) {
            singlePathSql.append(schemaTableTree.toOptionalLeftJoinWhereClause(mutableWhere));
        }

        //if partOfDuplicateQuery then the order by clause is on the outer select
        if (!partOfDuplicateQuery) {

            //group by
            SchemaTableTree lastSchemaTableTree = distinctQueryStack.getLast();
            if (!lastSchemaTableTree.groupBy.isEmpty() &&
                    !lastSchemaTableTree.groupBy.get(0).equals(T.label.getAccessor())) {

                singlePathSql.append(lastSchemaTableTree.toGroupByClause(this.sqlgGraph));
                if (!dropStep && lastOfPrevious == null && distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
                    singlePathSql.append(",\n\t");
                    singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                }
            } else if (lastSchemaTableTree.hasAggregateFunction() &&
                    !dropStep &&
                    lastOfPrevious == null &&
                    distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {

                singlePathSql.append("\nGROUP BY\n\t");
                singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
            }

            if (!dropStep && lastOfPrevious == null && distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
                singlePathSql.append("\nORDER BY\n\t");
                singlePathSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                mutableOrderBy.setTrue();
            }

            //construct the order by clause for the comparators
            for (SchemaTableTree schemaTableTree : distinctQueryStack) {
                singlePathSql.append(schemaTableTree.toOrderByClause(mutableOrderBy, -1));
                singlePathSql.append(schemaTableTree.toRangeClause(mutableOrderBy));
            }
        }

        currentColumnList.indexColumns(startIndexColumns);
        return singlePathSql.toString();
    }

    public boolean hasBulkWithinOrOut() {
        return this.hasContainers.stream().anyMatch(h -> SqlgUtil.isBulkWithinAndOut(sqlgGraph, h));
    }

    @SuppressWarnings("unchecked")
    private String bulkWithJoin() {
        StringBuilder sb = new StringBuilder();
        List<HasContainer> bulkHasContainers = this.hasContainers.stream().filter(h -> SqlgUtil.isBulkWithinAndOut(this.sqlgGraph, h)).toList();
        Map<String, PropertyDefinition> propertyDefinitionMap = this.filteredAllTables.get(toString());
        for (HasContainer hasContainer : bulkHasContainers) {
            P<List<Object>> predicate = (P<List<Object>>) hasContainer.getPredicate();
            Collection<Object> withInList = predicate.getValue();
            Set<Object> withInOuts = new HashSet<>(withInList);

            if (hasContainer.getBiPredicate() == Contains.within) {
                sb.append(" INNER JOIN\n\t");
            } else {
                //left join and in the where clause add a IS NULL, to find the values not in the right hand table
                sb.append(" LEFT JOIN\n\t");
            }
            sb.append("(VALUES ");
            boolean first = true;
            int identifierCount = 1;

            PropertyDefinition propertyDefinition = propertyDefinitionMap.get(hasContainer.getKey());
            for (Object withInOutValue : withInOuts) {
                if (withInOutValue == null) {
                    continue;
                }
                identifierCount = 1;
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append("(");
                if (withInOutValue instanceof RecordId recordId) {
                    if (!this.hasIDPrimaryKey) {
                        int count = 1;
                        for (Object identifier : recordId.getIdentifiers()) {
                            withInOutValue = identifier;
                            PropertyType propertyType = PropertyType.from(withInOutValue);
                            sb.append(sqlgGraph.getSqlDialect().valueToValuesString(propertyType, withInOutValue));
                            if (count++ < recordId.getIdentifiers().size()) {
                                sb.append(", ");
                            }
                            identifierCount++;
                        }
                    } else {
                        withInOutValue = recordId.sequenceId();
                        PropertyType propertyType = PropertyType.from(withInOutValue);
                        sb.append(sqlgGraph.getSqlDialect().valueToValuesString(propertyType, withInOutValue));
                    }
                } else {
                    PropertyType propertyType = propertyDefinition.propertyType();
                    sb.append(sqlgGraph.getSqlDialect().valueToValuesString(propertyType, withInOutValue));
                }
                sb.append(")");
            }
            sb.append(") as tmp");
            sb.append(this.rootSchemaTableTree().tmpTableAliasCounter);
            sb.append("(");
            if (hasContainer.getBiPredicate() == Contains.within) {
                if (identifierCount == 1) {
                    sb.append(WITHIN);
                } else {
                    for (int i = 1; i < identifierCount; i++) {
                        sb.append(WITHIN + "_").append(i);
                        if (i + 1 < identifierCount) {
                            sb.append(", ");
                        }
                    }
                }
            } else {
                if (identifierCount == 1) {
                    sb.append(WITHOUT);
                } else {
                    for (int i = 1; i < identifierCount; i++) {
                        sb.append(WITHOUT + "_").append(i);
                        if (i + 1 < identifierCount) {
                            sb.append(", ");
                        }
                    }
                }
            }
            sb.append(") ");
            sb.append(" on ");
            if (this.hasIDPrimaryKey || !hasContainer.getKey().equals(T.id.getAccessor())) {
                sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
                sb.append(".");
                sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
                sb.append(".");
                if (hasContainer.getKey().equals(T.id.getAccessor())) {
                    sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                } else {
                    sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey()));
                }
                if (hasContainer.getBiPredicate() == Contains.within) {
                    sb.append(" = tmp");
                    sb.append(this.rootSchemaTableTree().tmpTableAliasCounter++);
                    sb.append(".within");
                } else {
                    sb.append(" = tmp");
                    sb.append(this.rootSchemaTableTree().tmpTableAliasCounter++);
                    sb.append(".without");
                }
            } else {
                Preconditions.checkState(hasContainer.getKey().equals(T.id.getAccessor()));
                int count = 1;
                for (String identifier : this.getIdentifiers()) {
                    sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
                    sb.append(".");
                    sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
                    sb.append(".");
                    sb.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    if (hasContainer.getBiPredicate() == Contains.within) {
                        sb.append(" = tmp");
                        sb.append(this.rootSchemaTableTree().tmpTableAliasCounter);
                        sb.append(".within_").append(count);
                    } else {
                        sb.append(" = tmp");
                        sb.append(this.rootSchemaTableTree().tmpTableAliasCounter);
                        sb.append(".without_").append(count);
                    }
                    if (count++ < this.getIdentifiers().size()) {
                        sb.append(" AND ");
                    }
                }
                this.rootSchemaTableTree().tmpTableAliasCounter++;
            }
        }
        return sb.toString();
    }

    private String toOptionalLeftJoinWhereClause(MutableBoolean printedWhere) {
        final StringBuilder result = new StringBuilder();
        if (!printedWhere.booleanValue()) {
            printedWhere.setTrue();
            result.append("\nWHERE\n\t(");
        } else {
            result.append(" AND\n\t(");
        }
        if (this.drop) {
            Preconditions.checkState(this.parent.getSchemaTable().isEdgeTable(), "Optional left join drop queries must be for an edge!");
            if (parent.hasIDPrimaryKey) {
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
                result.append(".");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
                result.append(".");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                result.append(" IS NULL ");
            } else {
                int count = 1;
                for (String identifier : parent.getIdentifiers()) {
                    result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
                    result.append(".");
                    result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
                    result.append(".");
                    result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    result.append(" IS NULL ");
                    if (count++ < parent.getIdentifiers().size()) {
                        result.append(" AND\n\t");
                    }
                }
            }
            result.append(") AND\n\t");
            if (parent.hasIDPrimaryKey) {
                String rawLabel = this.getSchemaTable().getTable().substring(EDGE_PREFIX.length());
                result.append("(");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parent.getSchemaTable().getSchema()));
                result.append(".");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parent.getSchemaTable().getTable()));
                result.append(".");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                        this.getSchemaTable().getSchema() + "." + rawLabel +
                                (this.getDirection() == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                result.append(" IS NOT NULL)");

            } else {
                String rawLabel = this.getSchemaTable().getTable().substring(EDGE_PREFIX.length());
                result.append("(");
                int count = 1;
                for (String identifier : parent.getIdentifiers()) {
                    result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parent.getSchemaTable().getSchema()));
                    result.append(".");
                    result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parent.getSchemaTable().getTable()));
                    result.append(".");
                    result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                            this.getSchemaTable().getSchema() + "." + rawLabel + "." + identifier +
                                    (this.getDirection() == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                    result.append(" IS NOT NULL");
                    if (count++ < parent.getIdentifiers().size()) {
                        result.append(" AND\n\t");
                    }
                }
                result.append(")");
            }
        } else {
            Preconditions.checkState(this.parent.getSchemaTable().isVertexTable(), "Optional left join non drop queries must be for an vertex!");
            if (this.parent.hasIDPrimaryKey) {
                String rawLabel = this.parent.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
                result.append(".");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
                result.append(".");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                        this.parent.getSchemaTable().getSchema() + "." + rawLabel +
                                (this.getDirection() == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                result.append(" IS NULL)");
            } else {
                String rawLabel = this.parent.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
                result.append(".");
                result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
                result.append(".");
                int count = 1;
                for (String identifier : this.parent.getIdentifiers()) {
                    result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                            this.parent.getSchemaTable().getSchema() + "." + rawLabel +
                                    "." + identifier +
                                    (this.getDirection() == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                    result.append(" IS NULL");
                    if (count++ < this.parent.getIdentifiers().size()) {
                        result.append(") AND (");
                    }
                }
                result.append(")");

            }
        }
        return result.toString();
    }

    private String toWhereClause(MutableBoolean printedWhere) {
        final StringBuilder result = new StringBuilder();
        if (sqlgGraph.getSqlDialect().supportsBulkWithinOut()) {
            for (HasContainer hasContainer : this.hasContainers) {
                if (!SqlgUtil.isBulkWithin(sqlgGraph, hasContainer)) {
                    if (!printedWhere.booleanValue()) {
                        printedWhere.setTrue();
                        result.append("\nWHERE\n\t(");
                    } else {
                        result.append(" AND (");
                    }
                    WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
                    result.append(" ").append(whereClause.toSql(sqlgGraph, this, hasContainer)).append(")");
                }
            }
        } else {
            for (HasContainer hasContainer : this.getHasContainers()) {
                if (!printedWhere.booleanValue()) {
                    printedWhere.setTrue();
                    result.append("\nWHERE\n\t(");
                } else {
                    result.append(" AND (");
                }
                WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
                result.append(" ").append(whereClause.toSql(sqlgGraph, this, hasContainer)).append(")");
//                if (sqlgGraph.getSqlDialect().isMariaDb()) {
//                    result.append(" COLLATE latin1_general_cs");
//                }
            }
        }
        for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
            if (!printedWhere.booleanValue()) {
                printedWhere.setTrue();
                result.append("\nWHERE\n\t");
            } else {
                result.append(" AND ");
            }
            andOrHasContainer.toSql(sqlgGraph, this, result);
        }
        if (getParent() != null && !getParent().isOuterLeftJoin() && this.isOuterLeftJoin()) {
            if (!printedWhere.booleanValue()) {
                printedWhere.setTrue();
                result.append("\nWHERE\n\t");
            } else {
                result.append(" AND ");
            }
            String prefix = sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema());
            prefix += ".";
            prefix += sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());

            if (this.isHasIDPrimaryKey()) {
                result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                result.append(" IS NULL");
            } else {
                int i = 1;
                for (String identifier : this.getIdentifiers()) {
                    result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    result.append(" IS NULL");
                    if (i++ < this.getIdentifiers().size()) {
                        result.append(" AND ");
                    }
                }
            }
        }
        return result.toString();
    }

    private String toOrderByClause(MutableBoolean printedOrderBy, int counter) {
        StringBuilder result = new StringBuilder();
        for (org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>> comparator : this.dbComparators) {
            if (!printedOrderBy.booleanValue()) {
                printedOrderBy.setTrue();
                result.append("\nORDER BY\n\t");
            } else {
                result.append(",\n\t");
            }
            if (comparator.getValue1() instanceof ElementValueComparator<?> elementValueComparator) {
                String prefix = this.getSchemaTable().getSchema();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += this.getSchemaTable().getTable();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += elementValueComparator.getPropertyKey();
                String alias;
                if (counter == -1) {
                    //counter is -1 for single queries, i.e. they are not prefixed with ax
                    alias = sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getColumnNameAliasMap().get(prefix));
                } else {
                    alias = "a" + counter + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getColumnNameAliasMap().get(prefix));
                }
                result.append(" ").append(alias);
                if (elementValueComparator.getValueComparator() == Order.asc) {
                    result.append(" ASC");
                } else if (elementValueComparator.getValueComparator() == Order.desc) {
                    result.append(" DESC");
                } else {
                    throw new RuntimeException("Only handle Order.incr and Order.decr, not " + elementValueComparator.getValueComparator().toString());
                }

                //TODO redo this via SqlgOrderGlobalStep
            } else if ((comparator.getValue0() instanceof ValueTraversal<?, ?> || comparator.getValue0() instanceof TokenTraversal<?, ?>)
                    && comparator.getValue1() instanceof Order) {
                Traversal.Admin<?, ?> t = comparator.getValue0();
                String prefix = String.valueOf(this.stepDepth);
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += this.reducedLabels();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += this.getSchemaTable().getSchema();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += this.getSchemaTable().getTable();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                String key;
                if (t instanceof ValueTraversal<?, ?> elementValueTraversal) {
                    key = elementValueTraversal.getPropertyKey();
                } else {
                    TokenTraversal<?, ?> tokenTraversal = (TokenTraversal<?, ?>) t;
                    // see calculateLabeledAliasId
                    if (tokenTraversal.getToken().equals(T.id)) {
                        key = Topology.ID;
                    } else {
                        key = tokenTraversal.getToken().getAccessor();
                    }
                }
                prefix += key;
                String alias;
                String rawAlias = this.getColumnNameAliasMap().get(prefix);
                if (rawAlias == null) {
                    throw new IllegalArgumentException("order by field '" + prefix + "' not found!");
                }
                if (counter == -1) {
                    //counter is -1 for single queries, i.e. they are not prefixed with ax
                    alias = sqlgGraph.getSqlDialect().maybeWrapInQoutes(rawAlias);

                } else {
                    alias = "a" + counter + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(rawAlias);
                }
                result.append(" ").append(alias);
                if (comparator.getValue1() == Order.asc) {
                    result.append(" ASC");
                } else if (comparator.getValue1() == Order.desc) {
                    result.append(" DESC");
                } else {
                    throw new RuntimeException("Only handle Order.incr and Order.decr, not " + comparator.getValue1().toString());
                }
            } else {
                Preconditions.checkState(comparator.getValue0().getSteps().size() == 1, "toOrderByClause expects a TraversalComparator to have exactly one step!");
                Preconditions.checkState(comparator.getValue0().getSteps().get(0) instanceof SelectOneStep, "toOrderByClause expects a TraversalComparator to have exactly one SelectOneStep!");
                SelectOneStep<?, ?> selectOneStep = (SelectOneStep<?, ?>) comparator.getValue0().getSteps().get(0);
                Preconditions.checkState(selectOneStep.getScopeKeys().size() == 1, "toOrderByClause expects the selectOneStep to have one scopeKey!");
                Preconditions.checkState(selectOneStep.getLocalChildren().size() == 1, "toOrderByClause expects the selectOneStep to have one traversal!");
                Traversal.Admin<?, ?> t = (Traversal.Admin<?, ?>) selectOneStep.getLocalChildren().get(0);
                Preconditions.checkState(
                        t instanceof ValueTraversal ||
                                t instanceof TokenTraversal,
                        "toOrderByClause expects the selectOneStep's traversal to be a ElementValueTraversal or TokenTraversal!");

                //need to find the schemaTable that the select is for.
                //this schemaTable is for the leaf node as the order by only occurs last in gremlin (optimized gremlin that is)
                String select = (String) selectOneStep.getScopeKeys().iterator().next();
                SchemaTableTree selectSchemaTableTree = findSelectSchemaTable(select);
                Preconditions.checkState(selectSchemaTableTree != null, "SchemaTableTree not found for " + select);

                String prefix;
//                if (selectSchemaTableTree.children.isEmpty()) {
//                    //counter is -1 for single queries, i.e. they are not prefixed with ax
//                    prefix = String.valueOf(selectSchemaTableTree.stepDepth);
//                    prefix += SchemaTableTree.ALIAS_SEPARATOR;
//                } else {
                prefix = String.valueOf(selectSchemaTableTree.stepDepth);
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += selectSchemaTableTree.labels.iterator().next();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
//                }
                prefix += selectSchemaTableTree.getSchemaTable().getSchema();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += selectSchemaTableTree.getSchemaTable().getTable();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                if (t instanceof ValueTraversal<?, ?> elementValueTraversal) {
                    prefix += elementValueTraversal.getPropertyKey();
                } else {
                    TokenTraversal<?, ?> tokenTraversal = (TokenTraversal<?, ?>) t;
                    // see calculateLabeledAliasId
                    if (tokenTraversal.getToken().equals(T.id)) {
                        prefix += Topology.ID;
                    } else {
                        prefix += tokenTraversal.getToken().getAccessor();
                    }
                }
                String alias;
                String rawAlias = this.getColumnNameAliasMap().get(prefix);
                if (rawAlias == null) {
                    throw new IllegalArgumentException("order by field '" + prefix + "' not found!");
                }
                if (counter == -1) {
                    //counter is -1 for single queries, i.e. they are not prefixed with ax
                    alias = sqlgGraph.getSqlDialect().maybeWrapInQoutes(rawAlias);
                } else {
                    alias = "a" + selectSchemaTableTree.stepDepth + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(rawAlias);
                }
                result.append(" ").append(alias);
                if (comparator.getValue1() == Order.asc) {
                    result.append(" ASC");
                } else if (comparator.getValue1() == Order.desc) {
                    result.append(" DESC");
                } else {
                    throw new RuntimeException("Only handle Order.incr and Order.decr, not " + comparator.toString());
                }
            }
        }
        return result.toString();
    }

    private String toRangeClause(MutableBoolean mutableOrderBy) {
        if (this.sqlgRangeHolder != null && this.sqlgRangeHolder.isApplyOnDb()) {
            if (this.sqlgRangeHolder.hasRange()) {
                //This is MssqlServer, ugly but what to do???
                String sql = "";
                if (mutableOrderBy.isFalse() && sqlgGraph.getSqlDialect().isMssqlServer() && this.dbComparators.isEmpty()) {
                    sql = "\n\tORDER BY 1\n\t";
                }
                return sql + "\n" + sqlgGraph.getSqlDialect().getRangeClause(this.sqlgRangeHolder.getRange());
            } else {
                Preconditions.checkState(this.sqlgRangeHolder.hasSkip(), "If not a range query then it must be a skip.");
                return sqlgGraph.getSqlDialect().getSkipClause(this.sqlgRangeHolder.getSkip());
            }
        }
        return "";
    }

    private SchemaTableTree findSelectSchemaTable(String select) {
        return this.walkUp((t) -> t.stream().anyMatch(a -> a.endsWith(BaseStrategy.PATH_LABEL_SUFFIX + select)));
    }

    private SchemaTableTree walkUp(Predicate<Set<String>> predicate) {
        if (predicate.test(this.labels)) {
            return this;
        }
        if (this.parent != null) {
            return this.parent.walkUp(predicate);
        }
        return null;
    }

    public RecursiveRepeatStepConfig getRecursiveRepeatStepConfig() {
        return recursiveRepeatStepConfig;
    }

    public boolean isRecursiveQuery() {
        return this.recursiveRepeatStepConfig != null;
    }

    public boolean isPGRoutingQuery() {
        return this.pgRoutingConfig != null;
    }

    public void setUntilTraversalRootSchemaTableTree(SchemaTableTree untilTraversalRootSchemaTableTree) {
        this.untilTraversalRootSchemaTableTree = untilTraversalRootSchemaTableTree;
    }

    /**
     * Checks if the stack has the same element more than once.
     *
     * @param distinctQueryStack The stack to check for duplicates
     * @return true is there are duplicates else false
     */

    public boolean duplicatesInStack(LinkedList<SchemaTableTree> distinctQueryStack) {
        Set<SchemaTable> alreadyVisited = new HashSet<>();
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (!alreadyVisited.contains(schemaTableTree.getSchemaTable())) {
                alreadyVisited.add(schemaTableTree.getSchemaTable());
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * Constructs the 'from' clause with the required selected fields needed to make the join between the previous and the next SchemaTable
     *
     * @param columnList              Contains the columns
     * @param distinctQueryStack      //     * @param firstSchemaTableTree    This is the first SchemaTable in the current sql stack. If it is an Edge table then its foreign key
     *                                //     *                                field to the previous table need to be in the select clause in order for the join statement to
     *                                //     *                                reference it.
     *                                //     * @param lastSchemaTableTree
     * @param previousSchemaTableTree The previous schemaTableTree that will be joined to.
     * @param nextSchemaTableTree     represents the table to join to. it is null for the last table as there is nothing to join to.  @return
     */
    private String constructSelectClause(
            boolean dropStep,
            ColumnList columnList,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree previousSchemaTableTree,
            SchemaTableTree nextSchemaTableTree,
            boolean partOfDuplicateQuery) {

        SchemaTableTree firstSchemaTableTree = distinctQueryStack.getFirst();
        SchemaTableTree lastSchemaTableTree = distinctQueryStack.getLast();
        SchemaTable firstSchemaTable = firstSchemaTableTree.getSchemaTable();
        SchemaTable lastSchemaTable = lastSchemaTableTree.getSchemaTable();

        Preconditions.checkState(!(previousSchemaTableTree != null && previousSchemaTableTree.direction == Direction.BOTH), "Direction should never be BOTH");
        Preconditions.checkState(!(nextSchemaTableTree != null && nextSchemaTableTree.direction == Direction.BOTH), "Direction should never be BOTH");
        //The join is always between an edge and vertex or vertex and edge table.
        Preconditions.checkState(!(nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(VERTEX_PREFIX) && nextSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)), "Join can not be between 2 vertex tables!");
        Preconditions.checkState(!(nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(EDGE_PREFIX) && nextSchemaTableTree.getSchemaTable().getTable().startsWith(EDGE_PREFIX)), "Join can not be between 2 edge tables!");
        Preconditions.checkState(!(previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(VERTEX_PREFIX) && previousSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)), "Join can not be between 2 vertex tables!");
        Preconditions.checkState(!(previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(EDGE_PREFIX) && previousSchemaTableTree.getSchemaTable().getTable().startsWith(EDGE_PREFIX)), "Join can not be between 2 edge tables!");

        //join to the previous label/table
        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(EDGE_PREFIX)) {

            Preconditions.checkState(previousSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX), "Expected table to start with %s", VERTEX_PREFIX);
            String previousRawLabel = previousSchemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
            if (firstSchemaTableTree.direction == Direction.OUT) {
                if (previousSchemaTableTree.hasIDPrimaryKey) {
                    columnList.add(firstSchemaTable,
                            previousSchemaTableTree.getSchemaTable().getSchema() + "." + previousRawLabel + Topology.OUT_VERTEX_COLUMN_END,
                            previousSchemaTableTree.stepDepth,
                            firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction));
                } else {
                    ListOrderedSet<String> identifiers = previousSchemaTableTree.getIdentifiers();
                    for (String identifier : identifiers) {
                        if (previousSchemaTableTree.isDistributed() && previousSchemaTableTree.distributionColumn.equals(identifier)) {
                            columnList.add(firstSchemaTable,
                                    identifier,
                                    previousSchemaTableTree.stepDepth,
                                    firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction, identifier));
                        } else {
                            columnList.add(firstSchemaTable,
                                    previousSchemaTableTree.getSchemaTable().getSchema() + "." + previousRawLabel + "." + identifier + Topology.OUT_VERTEX_COLUMN_END,
                                    previousSchemaTableTree.stepDepth,
                                    firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction, identifier));
                        }
                    }
                }
            } else {
                if (previousSchemaTableTree.hasIDPrimaryKey) {
                    columnList.add(firstSchemaTable,
                            previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    previousRawLabel + Topology.IN_VERTEX_COLUMN_END,
                            previousSchemaTableTree.stepDepth,
                            firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction));
                } else {
                    ListOrderedSet<String> identifiers = previousSchemaTableTree.getIdentifiers();
                    for (String identifier : identifiers) {
                        if (previousSchemaTableTree.isDistributed() && previousSchemaTableTree.distributionColumn.equals(identifier)) {
                            columnList.add(firstSchemaTable,
                                    identifier,
                                    previousSchemaTableTree.stepDepth,
                                    firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction, identifier));
                        } else {
                            columnList.add(firstSchemaTable,
                                    previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            previousRawLabel + "." + identifier + Topology.IN_VERTEX_COLUMN_END,
                                    previousSchemaTableTree.stepDepth,
                                    firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction, identifier));
                        }
                    }
                }
            }
        } else if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            //if user defined identifiers then the regular properties make up the ids.
            if (firstSchemaTableTree.hasIDPrimaryKey) {
                columnList.add(firstSchemaTable, Topology.ID, firstSchemaTableTree.stepDepth, firstSchemaTableTree.calculatedAliasId());
            } else {
                ListOrderedSet<String> identifiers = firstSchemaTableTree.getIdentifiers();
                for (String identifier : identifiers) {
                    columnList.add(
                            firstSchemaTable,
                            identifier,
                            firstSchemaTableTree.stepDepth,
                            firstSchemaTableTree.calculateLabeledAliasPropertyName(identifier)
                    );
                }
            }
        }
        //join to the next table/label
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(EDGE_PREFIX)) {
            Preconditions.checkState(nextSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX), "Expected table to start with %s", VERTEX_PREFIX);

            String nextRawLabel = nextSchemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
            if (nextSchemaTableTree.isEdgeVertexStep()) {
                if (nextSchemaTableTree.hasIDPrimaryKey) {
                    columnList.add(lastSchemaTable,
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + (nextSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END),
                            nextSchemaTableTree.stepDepth,
                            lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + (nextSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END));

                } else {
                    ListOrderedSet<String> identifiers = nextSchemaTableTree.getIdentifiers();
                    for (String identifier : identifiers) {
                        if (nextSchemaTableTree.isDistributed() && nextSchemaTableTree.distributionColumn.equals(identifier)) {
                            columnList.add(lastSchemaTable,
                                    identifier,
                                    nextSchemaTableTree.stepDepth,
                                    lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + identifier);
                        } else {
                            columnList.add(lastSchemaTable,
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            nextRawLabel + "." + identifier + (nextSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END),
                                    nextSchemaTableTree.stepDepth,
                                    lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            nextRawLabel + "." + identifier + (nextSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END));
                        }
                    }
                }
                constructAllLabeledFromClause(false, distinctQueryStack, columnList);
            } else {
                if (nextSchemaTableTree.hasIDPrimaryKey) {
                    columnList.add(lastSchemaTable,
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." + nextRawLabel + (nextSchemaTableTree.direction == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END),
                            nextSchemaTableTree.stepDepth,
                            lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + (nextSchemaTableTree.direction == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END));
                } else {
                    ListOrderedSet<String> identifiers = nextSchemaTableTree.getIdentifiers();
                    for (String identifier : identifiers) {
                        if (nextSchemaTableTree.isDistributed() && nextSchemaTableTree.distributionColumn.equals(identifier)) {
                            columnList.add(lastSchemaTable,
                                    identifier,
                                    nextSchemaTableTree.stepDepth,
                                    lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + identifier);
                        } else {
                            columnList.add(lastSchemaTable,
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." + nextRawLabel + "." + identifier + (nextSchemaTableTree.direction == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END),
                                    nextSchemaTableTree.stepDepth,
                                    lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            nextRawLabel + "." + identifier + (nextSchemaTableTree.direction == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END));
                        }
                    }
                }
                constructAllLabeledFromClause(false, distinctQueryStack, columnList);
                constructEmitFromClause(distinctQueryStack, columnList);
            }
        } else if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            if (lastSchemaTableTree.hasIDPrimaryKey) {
                columnList.add(lastSchemaTable,
                        Topology.ID,
                        nextSchemaTableTree.stepDepth,
                        lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + Topology.ID);
            } else {
                ListOrderedSet<String> identifiers = lastSchemaTableTree.getIdentifiers();
                for (String identifier : identifiers) {
                    columnList.add(lastSchemaTable,
                            identifier,
                            nextSchemaTableTree.stepDepth,
                            lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + identifier);
                }
            }
            constructAllLabeledFromClause(false, distinctQueryStack, columnList);
        }

        //The last schemaTableTree in the call stack has no nextSchemaTableTree.
        //This last element's properties need to be returned, including all labeled properties for this path
        if (nextSchemaTableTree == null) {
            if (!dropStep && lastSchemaTableTree.getSchemaTable().isEdgeTable() && !lastSchemaTableTree.hasAggregateFunction()) {
                printEdgeInOutVertexIdFromClauseFor(firstSchemaTableTree, lastSchemaTableTree, columnList);
            }
            if (!lastSchemaTableTree.isOuterLeftJoin() && !lastSchemaTableTree.hasLabels()) {
                lastSchemaTableTree.addLabel(lastSchemaTableTree.getStepDepth() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_TEMP_FAKE_LABEL);
            }
            constructAllLabeledFromClause(dropStep, distinctQueryStack, columnList);
            constructEmitFromClause(distinctQueryStack, columnList);
        }
        return columnList.toSelectString(partOfDuplicateQuery);
    }

    private void constructAllLabeledFromClause(boolean dropStep, LinkedList<SchemaTableTree> distinctQueryStack, ColumnList columnList) {
        if (dropStep) {
            SchemaTableTree schemaTableTree = distinctQueryStack.getLast();
            printLabeledIDFromClauseFor(schemaTableTree, columnList);
            printLabeledFromClauseFor(schemaTableTree, columnList);
        } else {
            List<SchemaTableTree> labeled = distinctQueryStack.stream().filter(d -> !d.getLabels().isEmpty()).toList();
            for (SchemaTableTree schemaTableTree : labeled) {
                if (!schemaTableTree.hasAggregateFunction()) {
                    printLabeledIDFromClauseFor(schemaTableTree, columnList);
                }
                printLabeledFromClauseFor(schemaTableTree, columnList);
                if (schemaTableTree.getSchemaTable().isEdgeTable() && !schemaTableTree.hasAggregateFunction()) {
                    schemaTableTree.printLabeledEdgeInOutVertexIdFromClauseFor(columnList);
                }
            }
        }
    }

    /**
     * If emit is true then the edge id also needs to be printed.
     * This is required when there are multiple edges to the same vertex.
     * Only by having access to the edge id can one tell if the vertex needs to be emitted.
     */
    private void constructEmitFromClause(LinkedList<SchemaTableTree> distinctQueryStack, ColumnList columnList) {
        int count = 1;
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (schemaTableTree.parent != null && !schemaTableTree.getSchemaTable().isEdgeTable() && schemaTableTree.isEmit()) {
                //if the VertexStep is for an edge table there is no need to print edge ids as its already printed.
                //no need to print the first as its already present.
                if (count > 1) {
                    columnList.add(schemaTableTree.parent.getSchemaTable(), Topology.ID, schemaTableTree.parent.getStepDepth(), schemaTableTree.parent.calculatedAliasId());
                }
            }
            count++;
        }
    }

    private void printLabeledIDFromClauseFor(SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        if (lastSchemaTableTree.hasIDPrimaryKey) {
            String alias = cols.getAlias(lastSchemaTableTree, Topology.ID);
            if (alias == null) {
                alias = lastSchemaTableTree.calculateLabeledAliasId();
                cols.add(lastSchemaTableTree.getSchemaTable(), Topology.ID, lastSchemaTableTree.getStepDepth(), alias);
            } else {
                lastSchemaTableTree.calculateLabeledAliasId(alias);
            }
        } else {
            //first print the identifiers
            Map<String, PropertyDefinition> propertyDefinitionMap = lastSchemaTableTree.getFilteredAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
            ListOrderedSet<String> identifiers = lastSchemaTableTree.getIdentifiers();
            for (String identifier : identifiers) {
                printColumn(lastSchemaTableTree, cols, propertyDefinitionMap, identifier);
            }
        }
    }

    private static void printLabeledFromClauseFor(SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        //printing only a fake column now for count
        //if its part of a duplicate it will not be printed, but needs to be in the list for the outer select construction
        if (lastSchemaTableTree.hasAggregateFunction() &&
                lastSchemaTableTree.getAggregateFunction().getLeft().equals(GraphTraversal.Symbols.count)) {

            cols.add(
                    lastSchemaTableTree.getSchemaTable(),
                    "count",
                    lastSchemaTableTree.getStepDepth(),
                    "count",
                    lastSchemaTableTree.aggregateFunction.getLeft()
            );
        }
        Map<String, PropertyDefinition> propertyDefinitionMap = lastSchemaTableTree.getFilteredAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
        for (Map.Entry<String, PropertyDefinition> propertyDefinitionMapEntry : propertyDefinitionMap.entrySet()) {
            String col = propertyDefinitionMapEntry.getKey();
            if (lastSchemaTableTree.shouldSelectProperty(col)) {
                printColumn(lastSchemaTableTree, cols, propertyDefinitionMap, col);
            }
        }
    }

    private static void printColumn(SchemaTableTree lastSchemaTableTree, ColumnList cols, Map<String, PropertyDefinition> propertyDefinitionMap, String col) {
        String alias = cols.getAlias(lastSchemaTableTree, col);
        if (alias == null) {
            alias = lastSchemaTableTree.calculateLabeledAliasPropertyName(col);

            //count gets its own fake column
            if (lastSchemaTableTree.hasAggregateFunction() && !lastSchemaTableTree.getAggregateFunction().getLeft().equals("count") &&
                    (lastSchemaTableTree.aggregateFunction.getRight().isEmpty() || lastSchemaTableTree.aggregateFunction.getRight().contains(col))) {

                cols.add(
                        lastSchemaTableTree.getSchemaTable(),
                        col,
                        lastSchemaTableTree.getStepDepth(),
                        alias,
                        lastSchemaTableTree.aggregateFunction.getLeft()
                );
            } else {
                cols.add(
                        lastSchemaTableTree.getSchemaTable(),
                        col,
                        lastSchemaTableTree.getStepDepth(),
                        alias
                );
            }
        } else {
            lastSchemaTableTree.calculateLabeledAliasPropertyName(col, alias);
        }
        for (String postFix : propertyDefinitionMap.get(col).propertyType().getPostFixes()) {
            String postFixedCol = col + postFix;
            alias = cols.getAlias(lastSchemaTableTree, postFixedCol);
            // postfix do not use labeled methods
            if (alias == null) {
                alias = lastSchemaTableTree.calculateAliasPropertyName(postFixedCol);
                if (lastSchemaTableTree.hasAggregateFunction()) {
                    cols.add(
                            lastSchemaTableTree.getSchemaTable(),
                            postFixedCol,
                            lastSchemaTableTree.getStepDepth(),
                            alias,
                            lastSchemaTableTree.aggregateFunction.getLeft()
                    );
                } else {
                    cols.add(
                            lastSchemaTableTree.getSchemaTable(),
                            postFixedCol,
                            lastSchemaTableTree.getStepDepth(),
                            alias
                    );
                }
            }
        }
    }

    private void printEdgeInOutVertexIdFromClauseFor(SchemaTableTree firstSchemaTableTree, SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        Preconditions.checkState(lastSchemaTableTree.getSchemaTable().isEdgeTable());
        Set<ForeignKey> edgeForeignKeys = sqlgGraph.getTopology().getEdgeForeignKeys().get(lastSchemaTableTree.getSchemaTable().toString());
        for (ForeignKey edgeForeignKey : edgeForeignKeys) {
            if (firstSchemaTableTree == null || !firstSchemaTableTree.equals(lastSchemaTableTree) ||
                    firstSchemaTableTree.getDirection() != edgeForeignKey.getDirection()) {

                for (String foreignKey : edgeForeignKey.getCompositeKeys()) {
                    String alias = lastSchemaTableTree.calculateAliasPropertyName(foreignKey);
                    cols.addForeignKey(lastSchemaTableTree, foreignKey, alias);
                }
            }
        }
    }

    private void printLabeledEdgeInOutVertexIdFromClauseFor(ColumnList cols) {
        Preconditions.checkState(this.getSchemaTable().isEdgeTable());
        Set<ForeignKey> edgeForeignKeys = this.sqlgGraph.getTopology().getEdgeForeignKeys().get(this.getSchemaTable().toString());
        for (ForeignKey edgeForeignKey : edgeForeignKeys) {
            for (String foreignKey : edgeForeignKey.getCompositeKeys()) {
                String alias = cols.getAlias(this.getSchemaTable(), foreignKey, this.stepDepth, getAggregateFunction() == null ? null : getAggregateFunction().getLeft());
                if (alias == null) {
                    cols.addForeignKey(this, foreignKey, this.calculateLabeledAliasPropertyName(foreignKey));
                } else {
                    this.calculateLabeledAliasPropertyName(foreignKey, alias);
                }
            }
        }
    }

    private String calculatedAliasId() {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + Topology.ID;
        String alias = rootAliasAndIncrement();
        this.getColumnNameAliasMap().put(result, alias);
        this.getAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String calculateLabeledAliasId() {
        String reducedLabels = reducedLabels();
        String result = this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + Topology.ID;
        String alias = rootAliasAndIncrement();
        this.getColumnNameAliasMap().put(result, alias);
        this.getAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private void calculateLabeledAliasId(String alias) {
        String reducedLabels = reducedLabels();
        String result = this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + Topology.ID;
        this.getColumnNameAliasMap().put(result, alias);
        this.getAliasColumnNameMap().put(alias, result);
    }

    private String calculateLabeledAliasPropertyName(String propertyName) {
        String reducedLabels = reducedLabels();
        String result = this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        String alias = rootAliasAndIncrement();
        this.getColumnNameAliasMap().put(result, alias);
        this.getAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private void calculateLabeledAliasPropertyName(String propertyName, String alias) {
        String reducedLabels = reducedLabels();
        String result = this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        this.getColumnNameAliasMap().put(result, alias);
        this.getAliasColumnNameMap().put(alias, result);
    }

    private String calculateAliasPropertyName(String propertyName) {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        String alias = rootAliasAndIncrement();
        this.getColumnNameAliasMap().put(result, alias);
        this.getAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String calculatedAliasVertexForeignKeyColumnEnd(SchemaTableTree previousSchemaTableTree, Direction direction) {
        String previousRawLabel = previousSchemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() +
                ALIAS_SEPARATOR + previousSchemaTableTree.getSchemaTable().getSchema() +
                //This must be a dot as it's the foreign key column, i.e. blah__I
                "." + previousRawLabel + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END);
        String alias = rootAliasAndIncrement();
        this.getColumnNameAliasMap().put(result, alias);
        this.getAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String calculatedAliasVertexForeignKeyColumnEnd(SchemaTableTree previousSchemaTableTree, Direction direction, String identifier) {
        String previousRawLabel = previousSchemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() +
                ALIAS_SEPARATOR + previousSchemaTableTree.getSchemaTable().getSchema() +
                //This must be a dot as its the foreign key column, i.e. blah__I
                "." + previousRawLabel + "." + identifier + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END);
        String alias = rootAliasAndIncrement();
        this.getColumnNameAliasMap().put(result, alias);
        this.getAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String mappedAliasVertexForeignKeyColumnEnd(SchemaTableTree previousSchemaTableTree, Direction direction, String rawFromLabel) {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR +
                previousSchemaTableTree.getSchemaTable().getSchema() +
                //This must be a dot as its the foreign key column, i.e. blah__I
                "." + rawFromLabel + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END);
        return this.getColumnNameAliasMap().get(result);
    }

    private String mappedAliasVertexForeignKeyColumnEnd(SchemaTableTree previousSchemaTableTree, Direction direction, String rawFromLabel, String identifier) {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR +
                previousSchemaTableTree.getSchemaTable().getSchema() +
                //This must be a dot as its the foreign key column, i.e. blah__I
                "." + rawFromLabel + "." + identifier + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END);
        return this.getColumnNameAliasMap().get(result);
    }

    private String lastMappedAliasId() {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + Topology.ID;
        return this.getColumnNameAliasMap().get(result);
    }

    public String labeledAliasId() {
        if (this.labeledAliasId == null) {
            String reducedLabels = reducedLabels();
            this.labeledAliasId = this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + Topology.ID;
        }
        return this.labeledAliasId;
    }

    public String lastMappedAliasIdentifier(String identifier) {
        String reducedLabels = reducedLabels();
        String result = this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + identifier;
        return this.getColumnNameAliasMap().get(result);
    }

    private String labeledAliasIdentifier(String identifier) {
        String reducedLabels = reducedLabels();
        return this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + identifier;
    }

    private String rootAliasAndIncrement() {
        return "alias" + rootSchemaTableTree().rootAliasCounter++;
    }

    SchemaTableTree rootSchemaTableTree() {
        if (this.parent != null) {
            return this.parent.rootSchemaTableTree();
        } else {
            return this;
        }
    }

    private String reducedLabels() {
        if (this.reducedLabels == null) {
            this.reducedLabels = getLabels().stream().reduce((a, b) -> a + ALIAS_SEPARATOR + b).orElse("");
        }
        return this.reducedLabels;
    }

    private LinkedList<SchemaTableTree> constructQueryStackFromLeaf() {
        LinkedList<SchemaTableTree> queryCallStack = new LinkedList<>();
        SchemaTableTree node = this;
        while (node != null) {
            queryCallStack.add(0, node);
            node = node.parent;
        }
        return queryCallStack;
    }

    private String constructJoinBetweenSchemaTables(SchemaTableTree fromSchemaTableTree, SchemaTableTree labelToTraversTree, boolean leftJoin) {
        SchemaTable fromSchemaTable = fromSchemaTableTree.getSchemaTable();
        SchemaTable labelToTravers = labelToTraversTree.getSchemaTable();

        //Assert that this is always from vertex to edge table or edge to vertex table
        Preconditions.checkState(
                (fromSchemaTable.isVertexTable() && !labelToTravers.isVertexTable()) ||
                        (!fromSchemaTable.isVertexTable() && labelToTravers.isVertexTable())
        );

        String rawLabel;
        if (fromSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            rawLabel = fromSchemaTable.getTable().substring(VERTEX_PREFIX.length());
        } else {
            rawLabel = fromSchemaTable.getTable();
        }
        String rawLabelToTravers;
        if (labelToTravers.getTable().startsWith(VERTEX_PREFIX)) {
            rawLabelToTravers = labelToTravers.getTable().substring(VERTEX_PREFIX.length());
        } else {
            rawLabelToTravers = labelToTravers.getTable();
        }
        StringBuilder joinSql;
        if (leftJoin) {
            joinSql = new StringBuilder(" LEFT JOIN\n\t");
        } else {
            if (labelToTraversTree.isOuterLeftJoin()) {
                joinSql = new StringBuilder(" LEFT OUTER JOIN\n\t");
            } else {
                joinSql = new StringBuilder(" INNER JOIN\n\t");
            }
        }
        if (fromSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
            joinSql.append(".");
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
            joinSql.append(" ON ");
            if (fromSchemaTableTree.isHasIDPrimaryKey()) {
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema()));
                joinSql.append(".");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable()));
                joinSql.append(".");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                joinSql.append(" = ");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
                joinSql.append(".");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
                joinSql.append(".");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                        fromSchemaTable.getSchema() + "." + rawLabel +
                                (labelToTraversTree.getDirection() == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
            } else {
                int i = 1;
                for (String identifier : fromSchemaTableTree.getIdentifiers()) {
                    if (fromSchemaTableTree.isDistributed() && fromSchemaTableTree.distributionColumn.equals(identifier)) {
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema()));
                        joinSql.append(".");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable()));
                        joinSql.append(".");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        joinSql.append(" = ");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
                        joinSql.append(".");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
                        joinSql.append(".");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < fromSchemaTableTree.getIdentifiers().size()) {
                            joinSql.append(" AND ");
                        }
                    } else {
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema()));
                        joinSql.append(".");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable()));
                        joinSql.append(".");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        joinSql.append(" = ");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
                        joinSql.append(".");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
                        joinSql.append(".");
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                fromSchemaTable.getSchema() + "." + rawLabel + "." + identifier +
                                        (labelToTraversTree.getDirection() == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                        if (i++ < fromSchemaTableTree.getIdentifiers().size()) {
                            joinSql.append(" AND ");
                        }
                    }
                }
            }
        } else {
            //From edge to vertex table the foreign key is opposite to the direction.
            //This is because this is second part of the traversal via the edge.
            //This code did not take specific traversals from the edge into account.

            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
            joinSql.append(".");
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
            joinSql.append(" ON ");
            if (labelToTraversTree.isHasIDPrimaryKey()) {
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema()));
                joinSql.append(".");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable()));
                joinSql.append(".");
                if (labelToTraversTree.isEdgeVertexStep()) {
                    joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                            rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                } else {
                    joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                            rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                }
                joinSql.append(" = ");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
                joinSql.append(".");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
                joinSql.append(".");
                joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            } else {
                int i = 1;
                for (String identifier : labelToTraversTree.getIdentifiers()) {

                    joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema()));
                    joinSql.append(".");
                    joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable()));
                    joinSql.append(".");
                    if (fromSchemaTableTree.isDistributed() && fromSchemaTableTree.distributionColumn.equals(identifier)) {
                        if (labelToTraversTree.isEdgeVertexStep()) {
                            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        } else {
                            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        }
                    } else {
                        if (labelToTraversTree.isEdgeVertexStep()) {
                            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                                    rawLabelToTravers + "." + identifier + (labelToTraversTree.getDirection() == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                        } else {
                            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                                    rawLabelToTravers + "." + identifier + (labelToTraversTree.getDirection() == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                        }
                    }
                    joinSql.append(" = ");
                    joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
                    joinSql.append(".");
                    joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
                    joinSql.append(".");
                    joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    if (i++ < labelToTraversTree.getIdentifiers().size()) {
                        joinSql.append(" AND ");
                    }
                }
            }
        }
        return joinSql.toString();
    }

    private String appendToJoinBetweenSchemaTables(SchemaTableTree fromSchemaTableTree, SchemaTableTree labelToTraversTree) {
        SchemaTable fromSchemaTable = fromSchemaTableTree.getSchemaTable();
        SchemaTable labelToTravers = labelToTraversTree.getSchemaTable();

        //Assert that this is always from vertex to edge table or edge to vertex table
        Preconditions.checkState(
                (fromSchemaTable.isVertexTable() && !labelToTravers.isVertexTable()) ||
                        (!fromSchemaTable.isVertexTable() && labelToTravers.isVertexTable())
        );

        String rawLabel;
        if (fromSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            rawLabel = fromSchemaTable.getTable().substring(VERTEX_PREFIX.length());
        } else {
            rawLabel = fromSchemaTable.getTable();
        }
        String rawLabelToTravers;
        if (labelToTravers.getTable().startsWith(VERTEX_PREFIX)) {
            rawLabelToTravers = labelToTravers.getTable().substring(VERTEX_PREFIX.length());
        } else {
            rawLabelToTravers = labelToTravers.getTable();
        }
        String joinSql = " OR ";
        if (fromSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
            joinSql += " = ";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                    fromSchemaTable.getSchema() + "." + rawLabel +
                            (labelToTraversTree.getDirection() == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)
            );
        } else {
            //From edge to vertex table the foreign key is opposite to the direction.
            //This is because this is second part of the traversal via the edge.
            //This code did not take specific traversals from the edge into account.
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable());
            joinSql += ".";
            if (labelToTraversTree.isEdgeVertexStep()) {
                joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                        rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END));
            } else {
                joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                        rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END));
            }
            joinSql += " = ";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
        }
        return joinSql;
    }

    /**
     * Remove all leaf nodes that are not at the deepest level.
     * Those nodes are not to be included in the sql as they do not have enough incident edges.
     * i.e. The graph is not deep enough along those labels.
     * <p/>
     * This is done via a breath first traversal.
     */
    void removeAllButDeepestAndAddCacheLeafNodes(int depth) {
        Queue<SchemaTableTree> queue = new LinkedList<>();
        queue.add(this);
        while (!queue.isEmpty()) {
            SchemaTableTree current = queue.remove();
            if (current.stepDepth < depth && current.children.isEmpty() && !current.isEmit() && !current.isOptionalLeftJoin()) {
                removeNode(current);
            } else {
                queue.addAll(current.children);
                if ((current.stepDepth == depth && current.children.isEmpty()) || (current.isEmit() && current.children.isEmpty()) || current.isOptionalLeftJoin() && current.children.isEmpty()) {
                    this.leafNodes.add(current);
                }
            }
        }
    }

    private void removeNode(SchemaTableTree node) {
        SchemaTableTree parent = node.parent;
        if (parent != null) {
            parent.children.remove(node);
            this.leafNodes.remove(node);
            //check if the parent has any other children. if not it too can be deleted. Follow this pattern recursively up.
            if (parent.children.isEmpty()) {
                removeNode(parent);
            }
        }
    }

    boolean removeNodesInvalidatedByHas() {
        if (invalidateByHas(this)) {
            return true;
        } else {
            Queue<SchemaTableTree> queue = new LinkedList<>();
            queue.add(this);
            while (!queue.isEmpty()) {
                SchemaTableTree current = queue.remove();
                removeOrTransformHasContainers(current);
                if (invalidateByHas(current)) {
                    removeNode(current);
                } else {
                    queue.addAll(current.children);
                }
            }
            return false;
        }
    }

    boolean removeNodesInvalidatedByRestrictedProperties() {
        if (invalidateByRestrictedProperty(this)) {
            return true;
        } else {
            Queue<SchemaTableTree> queue = new LinkedList<>();
            queue.add(this);
            while (!queue.isEmpty()) {
                SchemaTableTree current = queue.remove();
                removeOrTransformHasContainers(current);
                if (invalidateByHas(current)) {
                    removeNode(current);
                } else {
                    queue.addAll(current.children);
                }
            }
            return false;
        }
    }

    /**
     * remove "has" containers that are not valid anymore
     * transform "has" containers that are equivalent to simpler statements.
     *
     * @param schemaTableTree the current table tree
     */
    private void removeOrTransformHasContainers(final SchemaTableTree schemaTableTree) {
        Set<HasContainer> toRemove = new HashSet<>();
        Set<HasContainer> toAdd = new HashSet<>();
        for (HasContainer hasContainer : schemaTableTree.hasContainers) {
            if (hasContainer.getKey().equals(label.getAccessor())) {
                toRemove.add(hasContainer);
            }

            if (Existence.NULL.equals(hasContainer.getBiPredicate())) {
                // we checked that a non existing property was null, that's fine
                if (!this.getFilteredAllTables().get(schemaTableTree.getSchemaTable().toString()).containsKey(hasContainer.getKey())) {
                    toRemove.add(hasContainer);
                }
            }
            if (Contains.without.equals(hasContainer.getBiPredicate())) {
                Object o = hasContainer.getValue();
                if (o instanceof Collection && ((Collection<?>) o).size() == 0) {
                    //P.without(Collections.emptySet()) translates to the sql IS NOT NULL
                    toRemove.add(hasContainer);
                    toAdd.add(new HasContainer(hasContainer.getKey(), new P<>(Existence.NOTNULL, null)));
                }
            }
        }
        schemaTableTree.hasContainers.removeAll(toRemove);
        schemaTableTree.hasContainers.addAll(toAdd);
    }

    @Override
    public String toString() {
        return this.schemaTable.toString();
    }

    @SuppressWarnings("unused")
    public String toTreeString() {
        StringBuilder result = new StringBuilder();
        internalToString(result);
        return result.toString();
    }

    private void internalToString(StringBuilder sb) {
        if (!sb.isEmpty()) {
            sb.append("\n");
        }
        sb.append("\t".repeat(Math.max(0, this.stepDepth)));
        sb.append(this.schemaTable.toString()).append(" ")
                .append(this.stepDepth).append(" ")
                .append(this.hasContainers.toString()).append(" ")
                .append("Comparators = ")
                .append(this.sqlgComparatorHolder.toString()).append(" ")
                .append("Range = ")
                .append(String.valueOf(this.sqlgRangeHolder.getRange())).append(" ")
                .append(this.direction != null ? this.direction.toString() : "").append(" ")
                .append("isVertexStep = ").append(this.isEdgeVertexStep())
                .append(" isUntilFirst = ").append(this.isUntilFirst())
                .append(" labels = ").append(this.labels);
        for (SchemaTableTree child : children) {
            child.internalToString(sb);
        }
    }

    private SchemaTableTree getParent() {
        return this.parent;
    }

    public Direction getDirection() {
        return this.direction;
    }

    public List<HasContainer> getHasContainers() {
        return this.hasContainers;
    }

    public List<HasContainer> getAdditionalPartitionHasContainers() {
        return additionalPartitionHasContainers;
    }

    public List<AndOrHasContainer> getAndOrHasContainers() {
        return andOrHasContainers;
    }

    public SqlgComparatorHolder getSqlgComparatorHolder() {
        return this.sqlgComparatorHolder;
    }

    public int getStepDepth() {
        return stepDepth;
    }

    public int getReplacedStepDepth() {
        return replacedStepDepth;
    }

    @SuppressWarnings("unused")
    public int testHashCode() {
        return //Objects.hashCode(this.parent) ^
                Objects.hashCode(this.schemaTable) ^
                        Objects.hashCode(this.direction) ^
                        Objects.hashCode(this.stepDepth) ^
                        Objects.hashCode(this.hasContainers) ^
                        Objects.hashCode(this.andOrHasContainers) ^
                        Objects.hashCode(this.sqlgComparatorHolder);
//                Objects.hashCode(this.dbComparators) ^
//                Objects.hashCode(this.sqlgRangeHolder) ^
//                Objects.hashCode(this.stepType) ^
//                Objects.hashCode(this.emit) ^
//                Objects.hashCode(this.untilFirst) ^
//                Objects.hashCode(this.optionalLeftJoin) ^
//                Objects.hashCode(this.drop) ^
//                Objects.hashCode(this.replacedStepDepth) ^
//                Objects.hashCode(this.labels) ^
//                Objects.hashCode(this.aggregateFunction) ^
//                Objects.hashCode(this.groupBy) ^
//                Objects.hashCode(this.idOnly);

    }

    /**
     * closing the SchemaTableTree indicates that this SchemaTableTree is complete and that no moore changing of its
     * state it allowed. This allows us to cache the hashCode for performance reasons.
     */
    public void close() {
        assert !this.closed : "close may only be called on an open SchemaTableTree";
        this.closed = true;
        this.hashCode = internalHashCode();
        this.hasIdentifierPrimaryKeyInHierarchy = hasIdentifierPrimaryKeyInHierarchy();
        for (SchemaTableTree child : this.children) {
            child.close();
        }
    }

    public boolean isHasIdentifierPrimaryKeyInHierarchy() {
        return hasIdentifierPrimaryKeyInHierarchy;
    }

    private boolean hasIdentifierPrimaryKeyInHierarchy() {
        if (!this.hasIDPrimaryKey) {
            return true;
        } else {
            for (SchemaTableTree child : this.children) {
                if (child.hasIdentifierPrimaryKeyInHierarchy()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public int hashCode() {
        if (this.closed) {
            return this.hashCode;
        } else {
            if (this.parent != null) {
                if (this.direction == null) {
                    return (this.schemaTable.toString() + this.parent).hashCode();
                } else {
                    return (this.schemaTable.toString() + this.direction.name() + this.parent).hashCode();
                }
            } else {
                if (this.direction == null) {
                    return this.schemaTable.toString().hashCode();
                } else {
                    return (this.schemaTable.toString() + this.direction.name()).hashCode();
                }
            }
        }
    }

    private int internalHashCode() {
        int hasContainerHashCode = 1;
        for (HasContainer hasContainer : this.hasContainers) {
            String key = hasContainer.getKey();
            hasContainerHashCode = 31 * hasContainerHashCode + (key == null ? 0 : key.hashCode());
            P<?> predicate = hasContainer.getPredicate();
            BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
            if (biPredicate == Contains.within || biPredicate == Contains.without) {
                //this logic is because of generating '?' for the parameters in the sql
                Collection<?> values = (Collection<?>) predicate.getValue();
                hasContainerHashCode = 31 * hasContainerHashCode + values.size();
            }
            hasContainerHashCode = 31 * hasContainerHashCode + biPredicate.hashCode();
        }
        return Objects.hash(
                this.parent,
                this.schemaTable,
                this.direction,
                this.stepDepth,
                hasContainerHashCode,
                this.andOrHasContainers,
                this.sqlgComparatorHolder,
                this.dbComparators,
                this.sqlgRangeHolder,
                this.stepType,
                this.emit,
                this.untilFirst,
                this.optionalLeftJoin,
                this.drop,
                this.replacedStepDepth,
                this.labels,
                this.aggregateFunction,
                this.groupBy,
                this.idOnly,
                this.identifiers,
                this.parentIdsAndIndexes,
                this.restrictedProperties
        );
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof SchemaTableTree other)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        boolean hasContainerEquals = this.hasContainers.size() == other.hasContainers.size();
        if (hasContainerEquals) {
            int count = 0;
            for (HasContainer hasContainer : this.hasContainers) {
                HasContainer otherHasContainer = other.hasContainers.get(count++);
                String key = hasContainer.getKey();
                String otherKey = otherHasContainer.getKey();
                BiPredicate<?, ?> biPredicate = hasContainer.getPredicate().getBiPredicate();
                BiPredicate<?, ?> otherBiPredicate = otherHasContainer.getPredicate().getBiPredicate();
                hasContainerEquals = key.equals(otherKey) && biPredicate.equals(otherBiPredicate);
                if (!hasContainerEquals) {
                    break;
                }
            }
        }
        return Objects.equals(this.parent, other.parent) &&
                Objects.equals(this.schemaTable, other.schemaTable) &&
                Objects.equals(this.direction, other.direction) &&
                Objects.equals(this.stepDepth, other.stepDepth) &&
                hasContainerEquals &&
                Objects.equals(this.andOrHasContainers, other.andOrHasContainers) &&
                Objects.equals(this.sqlgComparatorHolder, other.sqlgComparatorHolder) &&
                Objects.equals(this.dbComparators, other.dbComparators) &&
                Objects.equals(this.sqlgRangeHolder, other.sqlgRangeHolder) &&
                Objects.equals(this.stepType, other.stepType) &&
                Objects.equals(this.emit, other.emit) &&
                Objects.equals(this.untilFirst, other.untilFirst) &&
                Objects.equals(this.optionalLeftJoin, other.optionalLeftJoin) &&
                Objects.equals(this.drop, other.drop) &&
                Objects.equals(this.replacedStepDepth, other.replacedStepDepth) &&
                Objects.equals(this.labels, other.labels) &&
                Objects.equals(this.aggregateFunction, other.aggregateFunction) &&
                Objects.equals(this.groupBy, other.groupBy) &&
                Objects.equals(this.idOnly, other.idOnly) &&
                Objects.equals(this.identifiers, other.identifiers) &&
                Objects.equals(this.parentIdsAndIndexes, other.parentIdsAndIndexes) &&
                Objects.equals(this.restrictedProperties, other.restrictedProperties);
    }

    public Set<String> getLabels() {
        return this.labels;
    }

    public boolean hasLabels() {
        return !this.labels.isEmpty();
    }

    public Set<String> getRealLabelsCache() {
        if (this.realLabelsCache == null) {
            this.realLabelsCache = new HashSet<>();
            for (String label : this.labels) {
                if (label.contains(BaseStrategy.PATH_LABEL_SUFFIX)) {
                    this.realLabelsCache.add(label.substring(label.indexOf(BaseStrategy.PATH_LABEL_SUFFIX) + BaseStrategy.PATH_LABEL_SUFFIX.length()));
                } else if (label.contains(BaseStrategy.EMIT_LABEL_SUFFIX)) {
                    this.realLabelsCache.add(label.substring(label.indexOf(BaseStrategy.EMIT_LABEL_SUFFIX) + BaseStrategy.EMIT_LABEL_SUFFIX.length()));
                } else {
                    throw new IllegalStateException("label must contain " + BaseStrategy.PATH_LABEL_SUFFIX + " or " + BaseStrategy.EMIT_LABEL_SUFFIX);
                }
            }
        }
        return Collections.unmodifiableSet(this.realLabelsCache);
    }

    private boolean isEdgeVertexStep() {
        return this.stepType == STEP_TYPE.EDGE_VERTEX_STEP;
    }

    private boolean isVertexStep() {
        return this.stepType == STEP_TYPE.VERTEX_STEP;
    }

    public boolean isUntilFirst() {
        return untilFirst;
    }

    int getTmpTableAliasCounter() {
        return tmpTableAliasCounter;
    }

    public void loadProperty(ResultSet resultSet, SqlgElement sqlgElement) throws SQLException {
        List<ColumnList> rootColumnListStack = getRootColumnListStack();
        List<LinkedHashMap<ColumnList.Column, String>> _columns = new ArrayList<>();
        for (ColumnList columnList : rootColumnListStack) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(getStepDepth(), getSchemaTable());
            _columns.add(columns);
        }
        loadProperty(resultSet, sqlgElement, _columns);
    }

    public void loadProperty(ResultSet resultSet, SqlgElement sqlgElement, List<LinkedHashMap<ColumnList.Column, String>> columnsStack) throws SQLException {
        for (LinkedHashMap<ColumnList.Column, String> columns : columnsStack) {
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.getColumn().equals("index") && !column.isID() && !column.isForeignKey()) {
                    String propertyName = column.getColumn();
                    PropertyDefinition propertyDefinition = column.getPropertyDefinition();
                    boolean settedProperty;
                    if (column.getAggregateFunction() != null && column.getAggregateFunction().equalsIgnoreCase("avg")) {
                        settedProperty = sqlgElement.loadProperty(
                                resultSet,
                                propertyName,
                                column.getColumnIndex() - 1,
                                getColumnNameAliasMap(),
                                this.stepDepth,
                                PropertyDefinition.of(PropertyType.DOUBLE),
                                true);

                    } else if (column.getAggregateFunction() != null && column.getAggregateFunction().equalsIgnoreCase("sum")) {
                        PropertyDefinition becomes;
                        if (column.getPropertyDefinition().propertyType() == PropertyType.INTEGER || column.getPropertyDefinition().propertyType() == PropertyType.SHORT) {
                            becomes = PropertyDefinition.of(PropertyType.LONG);
                        } else {
                            becomes = column.getPropertyDefinition();
                        }
                        settedProperty = sqlgElement.loadProperty(
                                resultSet,
                                propertyName,
                                column.getColumnIndex(),
                                getColumnNameAliasMap(),
                                this.stepDepth,
                                becomes
                        );
                    } else if (column.getAggregateFunction() != null && column.getAggregateFunction().equals(GraphTraversal.Symbols.count)) {
                        PropertyDefinition becomes = PropertyDefinition.of(PropertyType.LONG);
                        settedProperty = sqlgElement.loadProperty(
                                resultSet,
                                propertyName,
                                column.getColumnIndex(),
                                getColumnNameAliasMap(),
                                this.stepDepth,
                                becomes
                        );
                    } else if (column.getAggregateFunction() != null) {
                        settedProperty = sqlgElement.loadProperty(
                                resultSet,
                                propertyName,
                                column.getColumnIndex(),
                                getColumnNameAliasMap(),
                                this.stepDepth,
                                propertyDefinition
                        );
                    } else {
                        settedProperty = sqlgElement.loadProperty(
                                resultSet,
                                propertyName,
                                column.getColumnIndex(),
                                getColumnNameAliasMap(),
                                this.stepDepth,
                                propertyDefinition
                        );
                    }
                    //Check if the query returned anything at all, if not default the aggregate result
                    if (!settedProperty && column.getAggregateFunction() != null) {
                        sqlgElement.internalSetProperty(propertyName, Double.NaN);
                    } else if (!settedProperty) {
                        sqlgElement.internalSetProperty(propertyName, null);
                    }
                }
            }

        }
    }

    public void loadEdgeInOutVertices(ResultSet resultSet, SqlgEdge sqlgEdge) throws SQLException {
        Preconditions.checkState(this.schemaTable.isEdgeTable());
        for (ColumnList columnList : this.getRootColumnListStack()) {
            Map<SchemaTable, List<ColumnList.Column>> inForeignKeyColumns = columnList.getInForeignKeys(this.stepDepth, this.schemaTable);
            for (Map.Entry<SchemaTable, List<ColumnList.Column>> schemaTableColumnsEntry : inForeignKeyColumns.entrySet()) {
                List<ColumnList.Column> columns = schemaTableColumnsEntry.getValue();
                if (columns.size() == 1 && !columns.get(0).isForeignKeyProperty()) {
                    ColumnList.Column column = columns.get(0);
                    sqlgEdge.loadInVertex(resultSet, column.getForeignSchemaTable(), column.getColumnIndex());
                } else {
                    sqlgEdge.loadInVertex(resultSet, columns);
                }
            }
            Map<SchemaTable, List<ColumnList.Column>> outForeignKeyColumns = columnList.getOutForeignKeys(this.stepDepth, this.schemaTable);
            for (Map.Entry<SchemaTable, List<ColumnList.Column>> schemaTableColumnsEntry : outForeignKeyColumns.entrySet()) {
                List<ColumnList.Column> columns = schemaTableColumnsEntry.getValue();
                if (columns.size() == 1 && !columns.get(0).isForeignKeyProperty()) {
                    ColumnList.Column column = columns.get(0);
                    sqlgEdge.loadOutVertex(resultSet, column.getForeignSchemaTable(), column.getColumnIndex());
                } else {
                    sqlgEdge.loadOutVertex(resultSet, columns);
                }
            }
        }
    }

    public List<Comparable> loadIdentifierObjects(Map<String, Integer> idColumnCountMap, ResultSet resultSet) throws SQLException {
        List<Comparable> identifierObjects = new ArrayList<>();
        for (String identifier : this.identifiers) {
            String labelledAliasIdentifier = labeledAliasIdentifier(identifier);
            Integer count = idColumnCountMap.get(labelledAliasIdentifier);
            identifierObjects.add((Comparable) resultSet.getObject(count));
        }
        return identifierObjects;
    }

    public boolean isLocalStep() {
        return this.localStep;
    }

    public boolean isLocalBarrierStep() {
        return localBarrierStep;
    }

    public boolean isFakeEmit() {
        return fakeEmit;
    }

    public void setFakeEmit(boolean fakeEmit) {
        this.fakeEmit = fakeEmit;
    }

    public STEP_TYPE getStepType() {
        return this.stepType;
    }

    public List<Pair<RecordId.ID, Long>> getParentIdsAndIndexes() {
        return this.parentIdsAndIndexes;
    }

    public void setParentIdsAndIndexes(List<Pair<RecordId.ID, Long>> parentIdsAndIndexes) {
        this.parentIdsAndIndexes = Collections.unmodifiableList(parentIdsAndIndexes);
    }

    public void removeDbComparators() {
        this.dbComparators.clear();
        for (SchemaTableTree child : this.children) {
            child.removeDbComparators();
        }
    }

    public boolean isDrop() {
        return this.drop;
    }

    public boolean isHasIDPrimaryKey() {
        return this.hasIDPrimaryKey;
    }

    public ListOrderedSet<String> getIdentifiers() {
        return this.identifiers;
    }

    public List<ColumnList> getRootColumnListStack() {
        return this.getAliasMapHolder().getColumnListStack();
    }

    private boolean isDistributed() {
        return this.distributionColumn != null;
    }

    public Set<String> getRestrictedProperties() {
        return restrictedProperties;
    }

    /**
     * should we select the given property?
     *
     * @param property the property name
     * @return true if the property should be part of the select clause, false otherwise
     */
    public boolean shouldSelectProperty(String property) {
        // no restriction
        if (getRoot().eagerLoad) {
            //this is set to true for in-memory order by clauses where the property being ordered on needs to be in the result-set.
            return true;
        } else if (this.getAggregateFunction() != null && this.getAggregateFunction().getLeft().equals(GraphTraversal.Symbols.count)) {
            if (this.groupBy.isEmpty() || this.groupBy.contains(T.label.getAccessor())) {
                return false;
            }
        }
        if (this.restrictedProperties.isEmpty() && this.idOnly) {
            return false;
        } else if (this.restrictedProperties.isEmpty()) {
            return true;
        } else {
            // explicit restriction
            if (!this.hasIDPrimaryKey && hasAggregateFunction()) {
                return this.restrictedProperties.contains(property);
            } else {
                return !this.identifiers.contains(property) && this.restrictedProperties.contains(property);
            }
        }
    }

    /**
     * calculate property restrictions from explicit restrictions and required properties
     */
    private void calculatePropertyRestrictions() {
        if (!this.restrictedProperties.isEmpty()) {
            // we use aliases for ordering, so we need the property in the select clause
            for (org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>> comparator : this.dbComparators) {

                if (comparator.getValue1() instanceof ElementValueComparator) {
                    this.restrictedProperties.add(((ElementValueComparator<?>) comparator.getValue1()).getPropertyKey());

                } else if ((comparator.getValue0() instanceof ValueTraversal<?, ?> || comparator.getValue0() instanceof TokenTraversal<?, ?>)
                        && comparator.getValue1() instanceof Order) {
                    Traversal.Admin<?, ?> t = comparator.getValue0();
                    String key;
                    if (t instanceof ValueTraversal<?, ?> elementValueTraversal) {
                        key = elementValueTraversal.getPropertyKey();
                    } else {
                        TokenTraversal<?, ?> tokenTraversal = (TokenTraversal<?, ?>) t;
                        // see calculateLabeledAliasId
                        if (tokenTraversal.getToken().equals(T.id)) {
                            key = Topology.ID;
                        } else {
                            key = tokenTraversal.getToken().getAccessor();
                        }
                    }
                    if (key != null) {
                        this.restrictedProperties.add(key);
                    }
                }
            }
        }
    }

    private boolean hasOnlyOneInOutEdgeLabel(SchemaTable schemaTable) {
        Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(schemaTable.getSchema());
        Preconditions.checkState(schemaOptional.isPresent(), "BUG: %s not found in the topology.", schemaTable.getSchema());
        Schema schema = schemaOptional.get();
        boolean result = true;
        if (schemaTable.isVertexTable()) {
            //Need to delete any in/out edges.
            Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(schemaTable.withOutPrefix().getTable());
            Preconditions.checkState(vertexLabelOptional.isPresent(), "BUG: %s not found in the topology.", schemaTable.withOutPrefix().getTable());
            VertexLabel vertexLabel = vertexLabelOptional.get();
            Collection<EdgeLabel> outEdgeLabels = vertexLabel.getOutEdgeLabels().values();
            for (EdgeLabel edgeLabel : outEdgeLabels) {
                result = edgeLabel.getOutVertexLabels().size() == 1;
                if (!result) {
                    break;
                }
            }
            if (result) {
                Collection<EdgeLabel> inEdgeLabels = vertexLabel.getInEdgeLabels().values();
                for (EdgeLabel edgeLabel : inEdgeLabels) {
                    result = edgeLabel.getInVertexLabels().size() == 1;
                    if (!result) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    private boolean hasNoEdgeLabels(SchemaTable schemaTable) {
        Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(schemaTable.getSchema());
        Preconditions.checkState(schemaOptional.isPresent(), "BUG: %s not found in the topology.", schemaTable.getSchema());
        Schema schema = schemaOptional.get();
        boolean result = true;
        if (schemaTable.isVertexTable()) {
            //Need to delete any in/out edges.
            Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(schemaTable.withOutPrefix().getTable());
            Preconditions.checkState(vertexLabelOptional.isPresent(), "BUG: %s not found in the topology.", schemaTable.withOutPrefix().getTable());
            VertexLabel vertexLabel = vertexLabelOptional.get();
            Collection<EdgeLabel> outEdgeLabels = vertexLabel.getOutEdgeLabels().values();
            Collection<EdgeLabel> inEdgeLabels = vertexLabel.getInEdgeLabels().values();
            result = outEdgeLabels.isEmpty() && inEdgeLabels.isEmpty();
        }
        return result;

    }

    public boolean hasAggregateFunction() {
        return this.aggregateFunction != null;
    }

    public Pair<String, List<String>> getAggregateFunction() {
        return this.aggregateFunction;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    private String toGroupByClause(SqlgGraph sqlgGraph) {
        return "\nGROUP BY\n\t" + this.groupBy.stream()
                .map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schemaTable.getSchema()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schemaTable.getTable()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(a))
                .reduce((a, b) -> a + ",\n\t" + b)
                .orElseThrow(IllegalStateException::new);
    }

    private String toOuterGroupByClause(SqlgGraph sqlgGraph, String queryCounter) {
        StringBuilder result = new StringBuilder("\nGROUP BY\n\t");
        for (String group : this.groupBy) {
            String prefix = String.valueOf(this.stepDepth);
            prefix += SchemaTableTree.ALIAS_SEPARATOR;
            prefix += this.reducedLabels();
            prefix += SchemaTableTree.ALIAS_SEPARATOR;
            prefix += this.getSchemaTable().getSchema();
            prefix += SchemaTableTree.ALIAS_SEPARATOR;
            prefix += this.getSchemaTable().getTable();
            prefix += SchemaTableTree.ALIAS_SEPARATOR;
            prefix += group;
            String alias = sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getColumnNameAliasMap().get(prefix));
            result.append(queryCounter);
            result.append(".");
            result.append(alias);
        }
        return result.toString();
    }

    public boolean hasLeafNodes() {
        return !this.leafNodes.isEmpty();
    }

    public enum STEP_TYPE {
        GRAPH_STEP,
        VERTEX_STEP,
        EDGE_VERTEX_STEP
    }

    private String constructRecursiveIncludeEdgeBothQueryWithoutNotStep(String startSql, LinkedList<SchemaTableTree> distinctQueryStack, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        SchemaTableTree vertexSchemaTableTree = distinctQueryStack.get(0);
        SchemaTableTree edgeSchemaTableTree = distinctQueryStack.get(1);
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable());

        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");

        String optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClause.replace("WHERE", " ");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("\n", "");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("v.\"depth\"", "\"depth\"");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "e"
        );

        optionalUntilWhereClause = optionalUntilWhereClause.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "previous_e"
        );
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");

        List<String> vertexColumns = new ArrayList<>();
        List<String> edgeColumns = new ArrayList<>();
        List<ColumnList> columnLists = getRootColumnListStack();
        for (ColumnList columnList : columnLists) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(0, vertexSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID()) {
                    vertexColumns.add(column.getColumn());
                }
            }
            columns = columnList.getFor(1, edgeSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID() && !column.isForeignKey()) {
                    edgeColumns.add(column.getColumn());
                }
            }
        }

        String vertexColumnsToAdd = vertexColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String edgeColumnsToAdd = edgeColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullVertexColumnsToAdd = vertexColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullEdgeColumnsToAdd = edgeColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        return """
                WITH start AS (
                    %s
                ), a AS (
                    WITH RECURSIVE search_tree("ID", {outForeignKey}, {inForeignKey}, depth, is_cycle, previous, path, epath, direction) AS (
                		WITH start_out as (
                			SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false,
                				ARRAY[e.{outForeignKey}],
                				ARRAY[e.{outForeignKey}, e.{inForeignKey}],
                				ARRAY[e."ID"],
                				'OUT'
                			FROM {edgeSchemaTable} e JOIN start ON start."alias1" = e.{outForeignKey}
                		), start_in as (
                			SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false,
                				ARRAY[e.{inForeignKey}],
                				ARRAY[e.{inForeignKey}, e.{outForeignKey}],
                				ARRAY[e."ID"],
                				'IN'
                			FROM {edgeSchemaTable} e JOIN start ON start."alias1" = e.{inForeignKey}
                		)
                		SELECT start_out.* FROM start_out UNION ALL SELECT start_in.* FROM start_in
                		UNION ALL
                		SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, st.depth + 1,
                			CASE
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN e.{inForeignKey} = ANY(path)
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN e.{outForeignKey} = ANY(path)
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN e.{outForeignKey} = ANY(path)
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN e.{inForeignKey} = ANY(path)
                			END,
                			CASE
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN path
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN path
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN path
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN path
                			END,
                			CASE
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN path || e.{inForeignKey}
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN path || e.{outForeignKey}
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN path || e.{outForeignKey}
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN path || e.{inForeignKey}
                			END,
                			epath || e."ID",
                			CASE
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN 'OUT'
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN 'IN'
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN 'IN'
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN 'OUT'
                			END
                		FROM
                		{edgeSchemaTable} e JOIN
                		search_tree st ON(
                			(st.direction = 'OUT' AND (st.{inForeignKey} = e.{outForeignKey} OR st.{inForeignKey} = e.{inForeignKey}))
                			OR
                			(st.direction = 'IN' AND (st.{outForeignKey} = e.{inForeignKey} OR st.{outForeignKey} = e.{outForeignKey}))) JOIN
                            {vertexSchemaTable} v ON
                            ((st.direction = 'OUT' AND (st.{inForeignKey} = v."ID"))
                            OR
                            (st.direction = 'IN' AND (st.{outForeignKey} = v."ID")))
                            JOIN
                            {edgeSchemaTable} previous_e ON st."ID" = previous_e."ID"
                		WHERE
                			NOT is_cycle {optionalUntilWhereClause}
                	)
                	SELECT *, gen_random_uuid() AS gen_random_uuid FROM search_tree
                	WHERE NOT is_cycle
                ), b AS (
                    SELECT * FROM a JOIN {vertexSchemaTable} v ON
                				((a.direction = 'OUT' AND (a.{inForeignKey} = v."ID"))
                				OR
                				(a.direction = 'IN' AND (a.{outForeignKey} = v."ID"))) JOIN
                 	        {edgeSchemaTable} e ON a."ID" = e."ID"
                    WHERE {optionalUntilWhereClauseWithoutNotStep}
                ), c AS (
                	SELECT 'vertex' as "type", b.path, b.gen_random_uuid FROM b
                	WHERE b.path NOT IN (SELECT previous from b)
                	UNION ALL
                	SELECT 'edge' as "type", b.epath, b.gen_random_uuid FROM b
                	WHERE b.path NOT IN (SELECT previous from b)
                ), d AS (
                    SELECT * FROM c JOIN UNNEST(c.path) WITH ORDINALITY AS cc(element_id, ordinal) ON c."type" = 'vertex' WHERE c."type" = 'vertex'
                    UNION ALL
                	SELECT * FROM c JOIN UNNEST(c.path) WITH ORDINALITY AS cc(element_id, ordinal) ON c."type" = 'edge' WHERE c."type" = 'edge'
                ), e AS (
                    SELECT d.path, type, "ID"{vertexColumns}, null as dummy{nullEdgeColumns}, ordinal, d.gen_random_uuid FROM d JOIN {vertexSchemaTable} AS _v on d.element_id = _v."ID" WHERE d.type = 'vertex'
                	UNION ALL
                    SELECT d.path, type, "ID"{nullVertexColumns}, null as dummy{edgeColumns}, ordinal, d.gen_random_uuid FROM d JOIN {edgeSchemaTable} AS _e on d.element_id = _e."ID" WHERE d.type = 'edge'
                )
                SELECT * from e
                ORDER BY gen_random_uuid, ordinal, type desc
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{vertexColumns}", vertexColumnsToAdd)
                .replace("{edgeColumns}", edgeColumnsToAdd)
                .replace("{nullVertexColumns}", nullVertexColumnsToAdd)
                .replace("{nullEdgeColumns}", nullEdgeColumnsToAdd)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{optionalUntilWhereClauseWithoutNotStep}", optionalUntilWhereClauseWithoutNotStep);

    }

    private String constructRecursiveIncludeEdgeBothQuery(String startSql, LinkedList<SchemaTableTree> distinctQueryStack, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        SchemaTableTree vertexSchemaTableTree = distinctQueryStack.get(0);
        SchemaTableTree edgeSchemaTableTree = distinctQueryStack.get(1);
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable());

        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        optionalUntilWhereClause = optionalUntilWhereClause.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "previous_e"
        );
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");

        List<String> vertexColumns = new ArrayList<>();
        List<String> edgeColumns = new ArrayList<>();
        List<ColumnList> columnLists = getRootColumnListStack();
        for (ColumnList columnList : columnLists) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(0, vertexSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID()) {
                    vertexColumns.add(column.getColumn());
                }
            }
            columns = columnList.getFor(1, edgeSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID() && !column.isForeignKey()) {
                    edgeColumns.add(column.getColumn());
                }
            }
        }

        String vertexColumnsToAdd = vertexColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String edgeColumnsToAdd = edgeColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullVertexColumnsToAdd = vertexColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullEdgeColumnsToAdd = edgeColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        return """
                WITH start AS (
                    %s
                ), a AS (
                    WITH RECURSIVE search_tree("ID", {outForeignKey}, {inForeignKey}, depth, is_cycle, previous, path, epath, direction) AS (
                		WITH start_out as (
                			SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false,
                				ARRAY[e.{outForeignKey}],
                				ARRAY[e.{outForeignKey}, e.{inForeignKey}],
                				ARRAY[e."ID"],
                				'OUT'
                			FROM {edgeSchemaTable} e JOIN start ON start."alias1" = e.{outForeignKey}
                		), start_in as (
                			SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false,
                				ARRAY[e.{inForeignKey}],
                				ARRAY[e.{inForeignKey}, e.{outForeignKey}],
                				ARRAY[e."ID"],
                				'IN'
                			FROM {edgeSchemaTable} e JOIN start ON start."alias1" = e.{inForeignKey}
                		)
                		SELECT start_out.* FROM start_out UNION ALL SELECT start_in.* FROM start_in
                		UNION ALL
                		SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, st.depth + 1,
                			CASE
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN e.{inForeignKey} = ANY(path)
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN e.{outForeignKey} = ANY(path)
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN e.{outForeignKey} = ANY(path)
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN e.{inForeignKey} = ANY(path)
                			END,
                			CASE
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN path
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN path
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN path
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN path
                			END,
                			CASE
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN path || e.{inForeignKey}
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN path || e.{outForeignKey}
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN path || e.{outForeignKey}
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN path || e.{inForeignKey}
                			END,
                			epath || e."ID",
                			CASE
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN 'OUT'
                			WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN 'IN'
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN 'IN'
                			WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN 'OUT'
                			END
                		FROM
                		{edgeSchemaTable} e JOIN
                		search_tree st ON(
                			(st.direction = 'OUT' AND (st.{inForeignKey} = e.{outForeignKey} OR st.{inForeignKey} = e.{inForeignKey}))
                			OR
                			(st.direction = 'IN' AND (st.{outForeignKey} = e.{inForeignKey} OR st.{outForeignKey} = e.{outForeignKey}))) JOIN
                            {vertexSchemaTable} v ON
                            ((st.direction = 'OUT' AND (st.{inForeignKey} = v."ID"))
                            OR
                            (st.direction = 'IN' AND (st.{outForeignKey} = v."ID")))
                            JOIN
                            {edgeSchemaTable} previous_e ON st."ID" = previous_e."ID"
                		WHERE
                			NOT is_cycle {optionalUntilWhereClause}
                	)
                	SELECT *, gen_random_uuid() AS gen_random_uuid FROM search_tree
                	WHERE NOT is_cycle
                ), b AS (
                	SELECT 'vertex' as "type", a.path, a.gen_random_uuid FROM a
                	WHERE a.path NOT IN (SELECT previous from a)
                	UNION ALL
                	SELECT 'edge' as "type", a.epath, a.gen_random_uuid FROM a
                	WHERE a.path NOT IN (SELECT previous from a)
                ), c AS (
                    SELECT * FROM b JOIN UNNEST(b.path) WITH ORDINALITY AS c(element_id, ordinal) ON b."type" = 'vertex' WHERE b."type" = 'vertex'
                    UNION ALL
                	SELECT * FROM b JOIN UNNEST(b.path) WITH ORDINALITY AS c(element_id, ordinal) ON b."type" = 'edge' WHERE b."type" = 'edge'
                ), d AS (
                    SELECT c.path, type, "ID"{vertexColumns}, null as dummy{nullEdgeColumns}, ordinal, c.gen_random_uuid FROM c JOIN {vertexSchemaTable} AS _v on c.element_id = _v."ID" WHERE c.type = 'vertex'
                	UNION ALL
                    SELECT c.path, type, "ID"{nullVertexColumns}, null as dummy{edgeColumns}, ordinal, c.gen_random_uuid FROM c JOIN {edgeSchemaTable} AS _e on c.element_id = _e."ID" WHERE c.type = 'edge'
                )
                SELECT * from d
                ORDER BY gen_random_uuid, ordinal, type desc
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{vertexColumns}", vertexColumnsToAdd)
                .replace("{edgeColumns}", edgeColumnsToAdd)
                .replace("{nullVertexColumns}", nullVertexColumnsToAdd)
                .replace("{nullEdgeColumns}", nullEdgeColumnsToAdd)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause);

    }

    private String constructRecursiveIncludeEdgeInQueryWithoutNotStep(String startSql, LinkedList<SchemaTableTree> distinctQueryStack, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        SchemaTableTree vertexSchemaTableTree = distinctQueryStack.get(0);
        SchemaTableTree edgeSchemaTableTree = distinctQueryStack.get(1);
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable());

        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");

        String optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClause.replace("WHERE", " ");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("\n", "");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("v.\"depth\"", "\"depth\"");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "e"
        );

        optionalUntilWhereClause = optionalUntilWhereClause.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "previous_e"
        );
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");

        List<String> vertexColumns = new ArrayList<>();
        List<String> edgeColumns = new ArrayList<>();
        List<ColumnList> columnLists = getRootColumnListStack();
        for (ColumnList columnList : columnLists) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(0, vertexSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID()) {
                    vertexColumns.add(column.getColumn());
                }
            }
            columns = columnList.getFor(1, edgeSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID() && !column.isForeignKey()) {
                    edgeColumns.add(column.getColumn());
                }
            }
        }

        String vertexColumnsToAdd = vertexColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String edgeColumnsToAdd = edgeColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullVertexColumnsToAdd = vertexColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullEdgeColumnsToAdd = edgeColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");

        return """
                WITH start AS (
                    %s
                ), a AS (
                	WITH RECURSIVE search_tree("ID", {inForeignKey}, {outForeignKey}, depth, is_cycle, previous, path, epath) AS (
                		SELECT
                            e."ID", e.{inForeignKey}, e.{outForeignKey}, 1, false, ARRAY[e.{inForeignKey}], ARRAY[e.{inForeignKey}, e.{outForeignKey}], ARRAY[e."ID"]
                        FROM
                            {edgeSchemaTable} e JOIN start ON start."alias1" = e.{inForeignKey}
                		UNION ALL
                		SELECT
                            e."ID", e.{inForeignKey}, e.{outForeignKey}, st.depth + 1, e.{outForeignKey} = ANY(path), path, path || e.{outForeignKey}, epath || e."ID"
                		FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON st.{outForeignKey} = e.{inForeignKey} JOIN
                            {vertexSchemaTable} v ON st.{outForeignKey} = v."ID" JOIN
                            {edgeSchemaTable} previous_e ON st."ID" = previous_e."ID"
                		WHERE NOT is_cycle {optionalUntilWhereClause}
                	)
                	SELECT *, gen_random_uuid() FROM search_tree WHERE NOT is_cycle
                ), b AS (
                 	SELECT * FROM a JOIN
                 	{vertexSchemaTable} v ON a.{outForeignKey} = v."ID" JOIN
                 	{edgeSchemaTable} e ON a."ID" = e."ID"
                 	WHERE {optionalUntilWhereClauseWithoutNotStep}
                ), c AS (
                	SELECT 'vertex' as "type", b.path, b.gen_random_uuid FROM b
                	WHERE b.path NOT IN (SELECT previous from b)
                	UNION ALL
                	SELECT 'edge' as "type", b.epath, b.gen_random_uuid FROM b 
                	WHERE b.path NOT IN (SELECT previous from b)
                ), d AS (
                    SELECT * FROM c JOIN UNNEST(c.path) WITH ORDINALITY AS cc(element_id, ordinal) ON c."type" = 'vertex' WHERE c."type" = 'vertex'
                    UNION ALL
                	SELECT * FROM c JOIN UNNEST(c.path) WITH ORDINALITY AS cc(element_id, ordinal) ON c."type" = 'edge' WHERE c."type" = 'edge'
                ), e AS (
                    SELECT d.path, type, "ID"{vertexColumns}, null as dummy{nullEdgeColumns}, ordinal, d.gen_random_uuid FROM d JOIN {vertexSchemaTable} AS _v on d.element_id = _v."ID" WHERE d.type = 'vertex'
                	UNION ALL
                    SELECT d.path, type, "ID"{nullVertexColumns}, null as dummy{edgeColumns}, ordinal, d.gen_random_uuid FROM d JOIN {edgeSchemaTable} AS _e on d.element_id = _e."ID" WHERE d.type = 'edge'
                )
                SELECT * from e 
                ORDER BY gen_random_uuid, ordinal, type desc
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{vertexColumns}", vertexColumnsToAdd)
                .replace("{edgeColumns}", edgeColumnsToAdd)
                .replace("{nullVertexColumns}", nullVertexColumnsToAdd)
                .replace("{nullEdgeColumns}", nullEdgeColumnsToAdd)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{optionalUntilWhereClauseWithoutNotStep}", optionalUntilWhereClauseWithoutNotStep);
    }

    private String constructRecursiveIncludeEdgeInQuery(String startSql, LinkedList<SchemaTableTree> distinctQueryStack, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        SchemaTableTree vertexSchemaTableTree = distinctQueryStack.get(0);
        SchemaTableTree edgeSchemaTableTree = distinctQueryStack.get(1);
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable());

        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        optionalUntilWhereClause = optionalUntilWhereClause.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "previous_e"
        );
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");

        List<String> vertexColumns = new ArrayList<>();
        List<String> edgeColumns = new ArrayList<>();
        List<ColumnList> columnLists = getRootColumnListStack();
        for (ColumnList columnList : columnLists) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(0, vertexSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID()) {
                    vertexColumns.add(column.getColumn());
                }
            }
            columns = columnList.getFor(1, edgeSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID() && !column.isForeignKey()) {
                    edgeColumns.add(column.getColumn());
                }
            }
        }

        String vertexColumnsToAdd = vertexColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String edgeColumnsToAdd = edgeColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullVertexColumnsToAdd = vertexColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullEdgeColumnsToAdd = edgeColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");

        return """
                WITH start AS (
                    %s
                ), a AS (
                	WITH RECURSIVE search_tree("ID", {inForeignKey}, {outForeignKey}, depth, is_cycle, previous, path, epath) AS (
                		SELECT
                            e."ID", e.{inForeignKey}, e.{outForeignKey}, 1, false, ARRAY[e.{inForeignKey}], ARRAY[e.{inForeignKey}, e.{outForeignKey}], ARRAY[e."ID"]
                        FROM
                            {edgeSchemaTable} e JOIN start ON start."alias1" = e.{inForeignKey}
                		UNION ALL
                		SELECT
                            e."ID", e.{inForeignKey}, e.{outForeignKey}, st.depth + 1, e.{outForeignKey} = ANY(path), path, path || e.{outForeignKey}, epath || e."ID"
                		FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON st.{outForeignKey} = e.{inForeignKey} JOIN
                            {vertexSchemaTable} v ON st.{outForeignKey} = v."ID" JOIN
                            {edgeSchemaTable} previous_e ON st."ID" = previous_e."ID"
                		WHERE NOT is_cycle {optionalUntilWhereClause}
                	)
                	SELECT *, gen_random_uuid() FROM search_tree WHERE NOT is_cycle
                ), b AS (
                	SELECT 'vertex' as "type", a.path, a.gen_random_uuid FROM a
                	WHERE a.path NOT IN (SELECT previous from a)
                	UNION ALL
                	SELECT 'edge' as "type", a.epath, a.gen_random_uuid FROM a
                	WHERE a.path NOT IN (SELECT previous from a)
                ), c AS (
                    SELECT * FROM b JOIN UNNEST(b.path) WITH ORDINALITY AS c(element_id, ordinal) ON b."type" = 'vertex' WHERE b."type" = 'vertex'
                    UNION ALL
                	SELECT * FROM b JOIN UNNEST(b.path) WITH ORDINALITY AS c(element_id, ordinal) ON b."type" = 'edge' WHERE b."type" = 'edge'
                ), d AS (
                    SELECT c.path, type, "ID"{vertexColumns}, null as dummy{nullEdgeColumns}, ordinal, c.gen_random_uuid FROM c JOIN {vertexSchemaTable} AS _v on c.element_id = _v."ID" WHERE c.type = 'vertex'
                	UNION ALL
                    SELECT c.path, type, "ID"{nullVertexColumns}, null as dummy{edgeColumns}, ordinal, c.gen_random_uuid FROM c JOIN {edgeSchemaTable} AS _e on c.element_id = _e."ID" WHERE c.type = 'edge'
                )
                SELECT * from d
                ORDER BY gen_random_uuid, ordinal, type desc
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{vertexColumns}", vertexColumnsToAdd)
                .replace("{edgeColumns}", edgeColumnsToAdd)
                .replace("{nullVertexColumns}", nullVertexColumnsToAdd)
                .replace("{nullEdgeColumns}", nullEdgeColumnsToAdd)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause);

    }

    private String constructRecursiveIncludeEdgeOutQueryWithoutNotStep(String startSql, LinkedList<SchemaTableTree> distinctQueryStack, String optionalUntilWhereClause) {
        Preconditions.checkState(this == distinctQueryStack.get(0));
        SchemaTableTree vertexSchemaTableTree = distinctQueryStack.get(0);
        SchemaTableTree edgeSchemaTableTree = distinctQueryStack.get(1);

        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable());

        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");

        String optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClause.replace("WHERE", " ");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("\n", "");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("v.\"depth\"", "\"depth\"");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "e"
        );

        optionalUntilWhereClause = optionalUntilWhereClause.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "previous_e"
        );
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");

        List<String> vertexColumns = new ArrayList<>();
        List<String> edgeColumns = new ArrayList<>();
        List<ColumnList> columnLists = getRootColumnListStack();
        for (ColumnList columnList : columnLists) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(0, vertexSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID()) {
                    vertexColumns.add(column.getColumn());
                }
            }
            columns = columnList.getFor(1, edgeSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID() && !column.isForeignKey()) {
                    edgeColumns.add(column.getColumn());
                }
            }
        }

        String vertexColumnsToAdd = vertexColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String edgeColumnsToAdd = edgeColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullVertexColumnsToAdd = vertexColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullEdgeColumnsToAdd = edgeColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");

        return """
                WITH start AS (
                    %s
                ), a AS (
                	WITH RECURSIVE search_tree("ID", {outForeignKey}, {inForeignKey}, depth, is_cycle, previous, path, epath) AS (
                		SELECT
                            e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false, ARRAY[e.{outForeignKey}], ARRAY[e.{outForeignKey}, e.{inForeignKey}], ARRAY[e."ID"]
                        FROM
                            {edgeSchemaTable} e JOIN start ON start."alias1" = e.{outForeignKey}
                		UNION ALL
                		SELECT
                            e."ID", e.{outForeignKey}, e.{inForeignKey}, st.depth + 1, e.{inForeignKey} = ANY(path), path, path || e.{inForeignKey}, epath || e."ID"
                		FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON st.{inForeignKey} = e.{outForeignKey} JOIN
                            {vertexSchemaTable} v ON st.{inForeignKey} = v."ID" JOIN
                            {edgeSchemaTable} previous_e ON st."ID" = previous_e."ID"
                		WHERE NOT is_cycle {optionalUntilWhereClause}
                	)
                	SELECT *, gen_random_uuid() FROM search_tree WHERE NOT is_cycle
                ), b AS (
                 	SELECT * FROM a JOIN\s
                 	{vertexSchemaTable} v ON a.{inForeignKey} = v."ID" JOIN
                 	{edgeSchemaTable} e ON a."ID" = e."ID"\s
                 	WHERE {optionalUntilWhereClauseWithoutNotStep}
                ), c AS (
                	SELECT 'vertex' as "type", b.path, b.gen_random_uuid FROM b
                	WHERE b.path NOT IN (SELECT previous from b)
                	UNION ALL
                	SELECT 'edge' as "type", b.epath, b.gen_random_uuid FROM b
                	WHERE b.path NOT IN (SELECT previous from b)
                ), d AS (
                    SELECT * FROM c JOIN UNNEST(c.path) WITH ORDINALITY AS cc(element_id, ordinal) ON c."type" = 'vertex' WHERE c."type" = 'vertex'
                    UNION ALL
                	SELECT * FROM c JOIN UNNEST(c.path) WITH ORDINALITY AS cc(element_id, ordinal) ON c."type" = 'edge' WHERE c."type" = 'edge'
                ), e AS (
                    SELECT d.path, type, "ID"{vertexColumns}, null as dummy{nullEdgeColumns}, ordinal, d.gen_random_uuid FROM d JOIN {vertexSchemaTable} AS _v on d.element_id = _v."ID" WHERE d.type = 'vertex'
                	UNION ALL
                    SELECT d.path, type, "ID"{nullVertexColumns}, null as dummy{edgeColumns}, ordinal, d.gen_random_uuid FROM d JOIN {edgeSchemaTable} AS _e on d.element_id = _e."ID" WHERE d.type = 'edge'
                )
                SELECT * from e
                ORDER BY gen_random_uuid, ordinal, type desc
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{vertexColumns}", vertexColumnsToAdd)
                .replace("{edgeColumns}", edgeColumnsToAdd)
                .replace("{nullVertexColumns}", nullVertexColumnsToAdd)
                .replace("{nullEdgeColumns}", nullEdgeColumnsToAdd)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{optionalUntilWhereClauseWithoutNotStep}", optionalUntilWhereClauseWithoutNotStep);
    }

    private String constructRecursiveIncludeEdgeOutQuery(String startSql, LinkedList<SchemaTableTree> distinctQueryStack, String optionalUntilWhereClause) {
        Preconditions.checkState(this == distinctQueryStack.get(0));
        SchemaTableTree vertexSchemaTableTree = distinctQueryStack.get(0);
        SchemaTableTree edgeSchemaTableTree = distinctQueryStack.get(1);

        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable());

        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        optionalUntilWhereClause = optionalUntilWhereClause.replace(
                this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTableTree.getSchemaTable().getTable()),
                "previous_e"
        );
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");

        List<String> vertexColumns = new ArrayList<>();
        List<String> edgeColumns = new ArrayList<>();
        List<ColumnList> columnLists = getRootColumnListStack();
        for (ColumnList columnList : columnLists) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(0, vertexSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID()) {
                    vertexColumns.add(column.getColumn());
                }
            }
            columns = columnList.getFor(1, edgeSchemaTableTree.getSchemaTable());
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.isID() && !column.isForeignKey()) {
                    edgeColumns.add(column.getColumn());
                }
            }
        }

        String vertexColumnsToAdd = vertexColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String edgeColumnsToAdd = edgeColumns.stream().map(a -> sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullVertexColumnsToAdd = vertexColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");
        String nullEdgeColumnsToAdd = edgeColumns.stream().map(a -> "null AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(a)).reduce((a, b) -> a + ", " + b).map(a -> ", " + a).orElse("");

        return """
                WITH start AS (
                    %s
                ), a AS (
                	WITH RECURSIVE search_tree("ID", {outForeignKey}, {inForeignKey}, depth, is_cycle, previous, path, epath) AS (
                		SELECT
                            e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false, ARRAY[e.{outForeignKey}], ARRAY[e.{outForeignKey}, e.{inForeignKey}], ARRAY[e."ID"]
                        FROM
                            {edgeSchemaTable} e JOIN start ON start."alias1" = e.{outForeignKey}
                		UNION ALL
                		SELECT
                            e."ID", e.{outForeignKey}, e.{inForeignKey}, st.depth + 1, e.{inForeignKey} = ANY(path), path, path || e.{inForeignKey}, epath || e."ID"
                		FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON st.{inForeignKey} = e.{outForeignKey} JOIN
                            {vertexSchemaTable} v ON st.{inForeignKey} = v."ID" JOIN
                            {edgeSchemaTable} previous_e ON st."ID" = previous_e."ID"
                		WHERE NOT is_cycle {optionalUntilWhereClause}
                	)
                	SELECT *, gen_random_uuid() FROM search_tree WHERE NOT is_cycle
                ), b AS (
                	SELECT 'vertex' as "type", a.path, a.gen_random_uuid FROM a
                	WHERE a.path NOT IN (SELECT previous from a)
                	UNION ALL
                	SELECT 'edge' as "type", a.epath, a.gen_random_uuid FROM a
                	WHERE a.path NOT IN (SELECT previous from a)
                ), c AS (
                    SELECT * FROM b JOIN UNNEST(b.path) WITH ORDINALITY AS c(element_id, ordinal) ON b."type" = 'vertex' WHERE b."type" = 'vertex'
                    UNION ALL
                	SELECT * FROM b JOIN UNNEST(b.path) WITH ORDINALITY AS c(element_id, ordinal) ON b."type" = 'edge' WHERE b."type" = 'edge'
                ), d AS (
                    SELECT c.path, type, "ID"{vertexColumns}, null as dummy{nullEdgeColumns}, ordinal, c.gen_random_uuid FROM c JOIN {vertexSchemaTable} AS _v on c.element_id = _v."ID" WHERE c.type = 'vertex'
                	UNION ALL
                    SELECT c.path, type, "ID"{nullVertexColumns}, null as dummy{edgeColumns}, ordinal, c.gen_random_uuid FROM c JOIN {edgeSchemaTable} AS _e on c.element_id = _e."ID" WHERE c.type = 'edge'
                )
                SELECT * from d
                ORDER BY gen_random_uuid, ordinal, type desc
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{vertexColumns}", vertexColumnsToAdd)
                .replace("{edgeColumns}", edgeColumnsToAdd)
                .replace("{nullVertexColumns}", nullVertexColumnsToAdd)
                .replace("{nullEdgeColumns}", nullEdgeColumnsToAdd)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause);

    }

    private String constructRecursiveOutQuery(String startSql, String select, String edge, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.EDGE_PREFIX + edge);

        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT ");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("v.\"depth\"", "\"depth\"");
        return """
                WITH start AS (
                    %s
                ), a AS (
                    WITH RECURSIVE search_tree("ID", {outForeignKey}, {inForeignKey}, depth, is_cycle, previous, path) AS (
                        SELECT
                            e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false, ARRAY[e.{outForeignKey}], ARRAY[e.{outForeignKey}, e.{inForeignKey}]
                        FROM
                            {edgeSchemaTable} e JOIN start ON start."alias1" = e.{outForeignKey}
                        UNION ALL
                        SELECT
                            e."ID", e.{outForeignKey}, e.{inForeignKey}, st.depth + 1, e.{inForeignKey} = ANY(path), path, path || e.{inForeignKey}
                        FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON st.{inForeignKey} = e.{outForeignKey} JOIN
                            {vertexSchemaTable} v ON st.{inForeignKey} = v."ID"
                        WHERE NOT is_cycle {optionalUntilWhereClause}
                    )
                    SELECT *, gen_random_uuid() FROM search_tree WHERE NOT is_cycle
                ), b AS (
                    SELECT a.gen_random_uuid, a.path, c.vertex_id, c.ordinal FROM a LEFT JOIN UNNEST(a.path) WITH ORDINALITY AS c(vertex_id, ordinal) ON true
                    WHERE a.path NOT IN (SELECT previous from a)
                )
                SELECT b.path, {select} from b JOIN {vertexSchemaTable} ON b.vertex_id = {vertexSchemaTable}."ID"
                ORDER BY b.gen_random_uuid, b.path, b.path, b.ordinal;
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{select}", select);
    }

    private String constructRecursiveInQueryWithoutNotStep(String startSql, String select, String edge, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + edge);
        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        String optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClause.replace("WHERE", " ");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("\n", "");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("v.\"depth\"", "\"depth\"");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT ");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("v.\"depth\"", "\"depth\"");
        return """
                WITH start AS (
                    %s
                ), a AS (
                    WITH RECURSIVE search_tree("ID", {inForeignKey}, {outForeignKey}, depth, is_cycle, previous, path) AS (
                        SELECT
                            e."ID", e.{inForeignKey}, e.{outForeignKey}, 1, false, ARRAY[e.{inForeignKey}], ARRAY[e.{inForeignKey}, e.{outForeignKey}]
                        FROM
                            {edgeSchemaTable} e JOIN start ON start."alias1" = e.{inForeignKey}
                        UNION ALL
                        SELECT
                            e."ID", e.{inForeignKey}, e.{outForeignKey}, st.depth + 1, e.{outForeignKey} = ANY(path), path, path || e.{outForeignKey}
                        FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON st.{outForeignKey} = e.{inForeignKey} JOIN
                            {vertexSchemaTable} v ON st.{outForeignKey} = v."ID"
                        WHERE NOT is_cycle {optionalUntilWhereClause}
                    )
                    SELECT *, gen_random_uuid() FROM search_tree WHERE NOT is_cycle
                ), b AS (
                    SELECT * FROM a JOIN "public"."V_Friend" v ON a."public.Friend__O" = v."ID"
                    WHERE {optionalUntilWhereClauseWithoutNotStep}
                ), c AS (
                    SELECT b.gen_random_uuid, b.path, cc.vertex_id, cc.ordinal FROM b LEFT JOIN UNNEST(b.path) WITH ORDINALITY AS cc(vertex_id, ordinal) ON true
                    WHERE b.path NOT IN (SELECT previous from b)
                )
                SELECT c.path, {select} from c JOIN {vertexSchemaTable} ON c.vertex_id = {vertexSchemaTable}."ID"
                ORDER BY c.gen_random_uuid, c.path, c.path, c.ordinal;
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{optionalUntilWhereClauseWithoutNotStep}", optionalUntilWhereClauseWithoutNotStep)
                .replace("{select}", select);
    }

    private String constructRecursiveOutQueryWithoutNotStep(String startSql, String select, String edge, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + edge);
        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        String optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClause.replace("WHERE", " ");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("\n", "");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("v.\"depth\"", "\"depth\"");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT ");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("v.\"depth\"", "\"depth\"");
        return """
                WITH start AS (
                    %s
                ), a AS (
                    WITH RECURSIVE search_tree("ID", {outForeignKey}, {inForeignKey}, depth, is_cycle, previous, path) AS (
                        SELECT
                            e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false, ARRAY[e.{outForeignKey}], ARRAY[e.{outForeignKey}, e.{inForeignKey}]
                        FROM
                            {edgeSchemaTable} e JOIN start ON start."alias1" = e.{outForeignKey}
                        UNION ALL
                        SELECT
                            e."ID", e.{outForeignKey}, e.{inForeignKey}, st.depth + 1, e.{inForeignKey} = ANY(path), path, path || e.{inForeignKey}
                        FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON st.{inForeignKey} = e.{outForeignKey} JOIN
                            {vertexSchemaTable} v ON st.{inForeignKey} = v."ID"
                        WHERE NOT is_cycle {optionalUntilWhereClause}
                    )
                    SELECT *, gen_random_uuid() FROM search_tree WHERE NOT is_cycle
                ), b AS (
                    SELECT * FROM a JOIN {vertexSchemaTable} v ON a."public.Friend__I" = v."ID"
                    WHERE {optionalUntilWhereClauseWithoutNotStep}
                ), c AS (
                    SELECT b.gen_random_uuid, b.path, cc.vertex_id, cc.ordinal FROM b LEFT JOIN UNNEST(b.path) WITH ORDINALITY AS cc(vertex_id, ordinal) ON true
                    WHERE b.path NOT IN (SELECT previous from b)
                )
                SELECT c.path, {select} from c JOIN {vertexSchemaTable} ON c.vertex_id = {vertexSchemaTable}."ID"
                ORDER BY c.gen_random_uuid, c.path, c.path, c.ordinal;
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{optionalUntilWhereClauseWithoutNotStep}", optionalUntilWhereClauseWithoutNotStep)
                .replace("{select}", select);
    }

    private String constructRecursiveInQuery(String startSql, String select, String edge, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + edge);
        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT ");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("v.\"depth\"", "\"depth\"");
        return """
                WITH start AS (
                    %s
                ), a AS (
                    WITH RECURSIVE search_tree("ID", {inForeignKey}, {outForeignKey}, depth, is_cycle, previous, path) AS (
                        SELECT
                            e."ID", e.{inForeignKey}, e.{outForeignKey}, 1, false, ARRAY[e.{inForeignKey}], ARRAY[e.{inForeignKey}, e.{outForeignKey}]
                        FROM
                            {edgeSchemaTable} e JOIN start ON start."alias1" = e.{inForeignKey}
                        UNION ALL
                        SELECT
                            e."ID", e.{inForeignKey}, e.{outForeignKey}, st.depth + 1, e.{outForeignKey} = ANY(path), path, path || e.{outForeignKey}
                        FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON st.{outForeignKey} = e.{inForeignKey} JOIN
                            {vertexSchemaTable} v ON st.{outForeignKey} = v."ID"
                        WHERE NOT is_cycle {optionalUntilWhereClause}
                    )
                    SELECT *, gen_random_uuid() FROM search_tree WHERE NOT is_cycle
                ), b AS (
                    SELECT a.gen_random_uuid, a.path, c.vertex_id, c.ordinal FROM a LEFT JOIN UNNEST(a.path) WITH ORDINALITY AS c(vertex_id, ordinal) ON true
                    WHERE a.path NOT IN (SELECT previous from a)
                )
                SELECT b.path, {select} from b JOIN {vertexSchemaTable} ON b.vertex_id = {vertexSchemaTable}."ID"
                ORDER BY b.gen_random_uuid, b.path, b.path, b.ordinal;
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{select}", select);
    }

    private String constructRecursiveBothQueryWithoutNotStep(String startSql, String select, String edge, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + edge);
        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        String optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClause.replace("WHERE", " ");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("\n", "");
        optionalUntilWhereClauseWithoutNotStep = optionalUntilWhereClauseWithoutNotStep.replace("v.\"depth\"", "\"depth\"");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT ");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("v.\"depth\"", "\"depth\"");
        return """
                WITH start as (
                    %s
                ), a AS (
                    WITH RECURSIVE search_tree("ID", {outForeignKey}, {inForeignKey}, depth, is_cycle, previous, path, direction) AS (
                        WITH start_out as (
                            SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false,
                                ARRAY[e.{outForeignKey}],
                                ARRAY[e.{outForeignKey}, e.{inForeignKey}],
                                'OUT'
                            FROM {edgeSchemaTable} e JOIN start ON start."alias1" = e.{outForeignKey}
                        ), start_in as (
                            SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false,
                                ARRAY[e.{inForeignKey}],
                                ARRAY[e.{inForeignKey}, e.{outForeignKey}],
                                'IN'
                            FROM {edgeSchemaTable} e JOIN start ON start."alias1" = e.{inForeignKey}
                        )
                        SELECT start_out.* FROM start_out UNION ALL SELECT start_in.* FROM start_in
                        UNION ALL
                        SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, st.depth + 1,
                            CASE
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN e.{inForeignKey} = ANY(path)
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN e.{outForeignKey} = ANY(path)
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN e.{outForeignKey} = ANY(path)
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN e.{inForeignKey} = ANY(path)
                            END,
                            CASE
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN path
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN path
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN path
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN path
                            END,
                            CASE
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN path || e.{inForeignKey}
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN path || e.{outForeignKey}
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN path || e.{outForeignKey}
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN path || e.{inForeignKey}
                            END,
                            CASE
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN 'OUT'
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN 'IN'
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN 'IN'
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN 'OUT'
                            END
                        FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON
                                ((st.direction = 'OUT' AND (st.{inForeignKey} = e.{outForeignKey} OR st.{inForeignKey} = e.{inForeignKey}))
                                OR
                                 (st.direction = 'IN' AND (st.{outForeignKey} = e.{inForeignKey} OR st.{outForeignKey} = e.{outForeignKey}))) JOIN
                            {vertexSchemaTable} v ON
                                ((st.direction = 'OUT' AND (st.{inForeignKey} = v."ID"))
                                OR
                                (st.direction = 'IN' AND (st.{outForeignKey} = v."ID")))
                        WHERE
                            NOT is_cycle {optionalUntilWhereClause}
                    )
                    SELECT *, gen_random_uuid() FROM search_tree
                    WHERE NOT is_cycle
                ), b AS (
                    SELECT * FROM a JOIN {vertexSchemaTable} v ON\s
                				((a.direction = 'OUT' AND (a.{inForeignKey} = v."ID"))
                				OR
                				(a.direction = 'IN' AND (a.{outForeignKey} = v."ID")))
                    WHERE {optionalUntilWhereClauseWithoutNotStep}
                ), c AS (
                    SELECT b.gen_random_uuid, b.path, cc.vertex_id, cc.ordinal FROM b LEFT JOIN UNNEST(b.path) WITH ORDINALITY AS cc(vertex_id, ordinal) ON true
                    WHERE b.path NOT IN (SELECT previous from b)
                )
                SELECT c.path, {select} from c JOIN {vertexSchemaTable} ON c.vertex_id = {vertexSchemaTable}."ID"
                ORDER BY c.gen_random_uuid, c.path, c.ordinal
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{optionalUntilWhereClauseWithoutNotStep}", optionalUntilWhereClauseWithoutNotStep)
                .replace("{select}", select);
    }

    private String constructRecursiveBothQuery(String startSql, String select, String edge, String optionalUntilWhereClause) {
        String inForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.IN_VERTEX_COLUMN_END);
        String outForeignKey = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema() + "." + getSchemaTable().withOutPrefix().getTable() + Topology.OUT_VERTEX_COLUMN_END);
        String vertexSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getTable());
        String edgeSchemaTable = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchemaTable().getSchema()) + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.EDGE_PREFIX + edge);
        optionalUntilWhereClause = optionalUntilWhereClause.replace(vertexSchemaTable, "v");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("WHERE", "AND NOT ");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("\n", "");
        optionalUntilWhereClause = optionalUntilWhereClause.replace("v.\"depth\"", "\"depth\"");
        return """
                WITH start as (
                    %s
                ), a AS (
                    WITH RECURSIVE search_tree("ID", {outForeignKey}, {inForeignKey}, depth, is_cycle, previous, path, direction) AS (
                        WITH start_out as (
                            SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false,
                                ARRAY[e.{outForeignKey}],
                                ARRAY[e.{outForeignKey}, e.{inForeignKey}],
                                'OUT'
                            FROM {edgeSchemaTable} e JOIN start ON start."alias1" = e.{outForeignKey}
                        ), start_in as (
                            SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, 1, false,
                                ARRAY[e.{inForeignKey}],
                                ARRAY[e.{inForeignKey}, e.{outForeignKey}],
                                'IN'
                            FROM {edgeSchemaTable} e JOIN start ON start."alias1" = e.{inForeignKey}
                        )
                        SELECT start_out.* FROM start_out UNION ALL SELECT start_in.* FROM start_in
                        UNION ALL
                        SELECT e."ID", e.{outForeignKey}, e.{inForeignKey}, st.depth + 1,
                            CASE
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN e.{inForeignKey} = ANY(path)
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN e.{outForeignKey} = ANY(path)
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN e.{outForeignKey} = ANY(path)
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN e.{inForeignKey} = ANY(path)
                            END,
                            CASE
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN path
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN path
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN path
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN path
                            END,
                            CASE
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN path || e.{inForeignKey}
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN path || e.{outForeignKey}
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN path || e.{outForeignKey}
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN path || e.{inForeignKey}
                            END,
                            CASE
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{outForeignKey} THEN 'OUT'
                            WHEN st.direction = 'OUT' AND st.{inForeignKey} = e.{inForeignKey} THEN 'IN'
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{inForeignKey} THEN 'IN'
                            WHEN st.direction = 'IN' AND st.{outForeignKey} = e.{outForeignKey} THEN 'OUT'
                            END
                        FROM
                            {edgeSchemaTable} e JOIN
                            search_tree st ON
                                ((st.direction = 'OUT' AND (st.{inForeignKey} = e.{outForeignKey} OR st.{inForeignKey} = e.{inForeignKey}))
                                OR
                                 (st.direction = 'IN' AND (st.{outForeignKey} = e.{inForeignKey} OR st.{outForeignKey} = e.{outForeignKey}))) JOIN
                            {vertexSchemaTable} v ON
                                ((st.direction = 'OUT' AND (st.{inForeignKey} = v."ID"))
                                OR
                                (st.direction = 'IN' AND (st.{outForeignKey} = v."ID")))
                        WHERE
                            NOT is_cycle {optionalUntilWhereClause}
                    )
                    SELECT *, gen_random_uuid() FROM search_tree
                    WHERE NOT is_cycle
                ), b AS (
                    SELECT a.gen_random_uuid, a.path, vertex_id, ordinal FROM a LEFT JOIN UNNEST(a.path) WITH ORDINALITY AS b(vertex_id, ordinal) ON true
                    WHERE a.path NOT IN (SELECT previous from a)
                    ORDER BY a.gen_random_uuid, a.path, ordinal
                )
                SELECT b.path, {select} from b JOIN {vertexSchemaTable} ON b.vertex_id = {vertexSchemaTable}."ID";
                """
                .formatted(startSql)
                .replace("{outForeignKey}", outForeignKey)
                .replace("{inForeignKey}", inForeignKey)
                .replace("{vertexSchemaTable}", vertexSchemaTable)
                .replace("{edgeSchemaTable}", edgeSchemaTable)
                .replace("{optionalUntilWhereClause}", optionalUntilWhereClause)
                .replace("{select}", select);
    }

    public SchemaTableTree schemaTableTreeForLabel(String label) {
        if (getLabels().contains(label)) {
            return this;
        } else {
            return this.parent.schemaTableTreeForLabel(label);
        }
    }
}
