package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
    private int stepDepth;
    private SchemaTable schemaTable;
    private SchemaTableTree parent;
    //The root node does not have a direction. For the other nodes it indicates the direction from its parent to it.
    private Direction direction;
    private STEP_TYPE stepType;
    private List<SchemaTableTree> children = new ArrayList<>();
    private SqlgGraph sqlgGraph;
    //leafNodes is only set on the root node;
    private List<SchemaTableTree> leafNodes = new ArrayList<>();
    private List<HasContainer> hasContainers;
    private List<AndOrHasContainer> andOrHasContainers;
    private SqlgComparatorHolder sqlgComparatorHolder = new SqlgComparatorHolder();
    private List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> dbComparators;
    //labels are immutable
    private Set<String> labels;
    private Set<String> realLabels;
    private String reducedLabels;
    //untilFirst is for the repeatStep optimization
    private boolean untilFirst;

    //This counter must only ever be used on the root node of the schema table tree
    //It is used to alias the select clauses
    private int rootAliasCounter = 1;
    private boolean emit;

    //left join, as required by the optimized ChooseStep via the optional step
    private boolean optionalLeftJoin;

    //Only root SchemaTableTrees have these maps;
    private AliasMapHolder aliasMapHolder;

    //This counter is used for the within predicate when aliasing the temporary table
    private int tmpTableAliasCounter = 1;

    //This represents all tables filtered by TopologyStrategy
    private Map<String, Map<String, PropertyType>> filteredAllTables;

    private int replacedStepDepth;

    //Cached for query load performance
    private Map<String, Pair<String, PropertyType>> columnNamePropertyName;
    private String idProperty;
    private String labeledAliasId;
    private boolean hasIDPrimaryKey;
    private ListOrderedSet<String> identifiers;

    private boolean localStep = false;
    private boolean fakeEmit = false;

    /**
     * Indicates the DropStep.
     */
    private boolean drop;

    /**
     * range limitation, if any
     */
    private SqlgRangeHolder sqlgRangeHolder;
    //This is the incoming element id and the traversals start elements index, for SqlgVertexStep.
    private List<Pair<Long, Long>> parentIdsAndIndexes;

    private List<ColumnList> columnListStack = new ArrayList<>();

    public void removeTopologyStrategyHasContainer() {
        SqlgUtil.removeTopologyStrategyHasContainer(this.hasContainers);
        for (SchemaTableTree child : children) {
            SqlgUtil.removeTopologyStrategyHasContainer(this.hasContainers);
            child.removeTopologyStrategyHasContainer();
        }
    }

    public enum STEP_TYPE {
        GRAPH_STEP,
        VERTEX_STEP,
        EDGE_VERTEX_STEP
    }

    SchemaTableTree(SqlgGraph sqlgGraph, SchemaTable schemaTable, int stepDepth, int replacedStepDepth) {
        this.sqlgGraph = sqlgGraph;
        this.schemaTable = schemaTable;
        this.stepDepth = stepDepth;
        this.hasContainers = new ArrayList<>();
        this.andOrHasContainers = new ArrayList<>();
        this.dbComparators = new ArrayList<>();
        this.labels = Collections.emptySet();
        this.replacedStepDepth = replacedStepDepth;
        this.filteredAllTables = SqlgUtil.filterSqlgSchemaHasContainers(sqlgGraph.getTopology(), this.hasContainers, Topology.SQLG_SCHEMA.equals(schemaTable.getSchema()));
        this.identifiers = setIdentifiers();
        this.hasIDPrimaryKey = this.identifiers.isEmpty();
    }

    /**
     * This constructor is called for the root SchemaTableTree(s)
     * <p>
     * This is invoked from {@link ReplacedStep} when creating the root {@link SchemaTableTree}s.
     * The hasContainers at this stage contains the {@link TopologyStrategy} from or without hasContainer.
     * After doing the filtering it must be removed from the hasContainers as it must not partake in sql generation.
     */
    SchemaTableTree(SqlgGraph sqlgGraph,
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
                    Set<String> labels
    ) {
        this.sqlgGraph = sqlgGraph;
        this.schemaTable = schemaTable;
        this.stepDepth = stepDepth;
        this.hasContainers = hasContainers;
        this.andOrHasContainers = andOrHasContainers;
        this.replacedStepDepth = replacedStepDepth;
        this.sqlgComparatorHolder = sqlgComparatorHolder;
        this.dbComparators = dbComparators;
        this.sqlgRangeHolder = sqlgRangeHolder;
        this.labels = Collections.unmodifiableSet(labels);
        this.stepType = stepType;
        this.emit = emit;
        this.untilFirst = untilFirst;
        this.optionalLeftJoin = optionalLeftJoin;
        this.drop = drop;
        this.filteredAllTables = SqlgUtil.filterSqlgSchemaHasContainers(sqlgGraph.getTopology(), this.hasContainers, Topology.SQLG_SCHEMA.equals(schemaTable.getSchema()));
        this.identifiers = setIdentifiers();
        this.hasIDPrimaryKey = this.identifiers.isEmpty();
        initializeAliasColumnNameMaps();
    }

    private ListOrderedSet<String> setIdentifiers() {
        if (this.schemaTable.isVertexTable()) {
            VertexLabel vertexLabel = this.sqlgGraph.getTopology().getVertexLabel(
                    this.schemaTable.withOutPrefix().getSchema(),
                    this.schemaTable.withOutPrefix().getTable()
            ).orElseThrow(() -> new IllegalStateException(String.format("Label %s must ne present.", this.schemaTable.toString())));
            return vertexLabel.getIdentifiers();
        } else {
            EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getEdgeLabel(
                    this.schemaTable.withOutPrefix().getSchema(),
                    this.schemaTable.withOutPrefix().getTable()
            ).orElseThrow(() -> new IllegalStateException(String.format("Label %s must ne present.", this.schemaTable.toString())));
            return edgeLabel.getIdentifiers();
        }
    }

    SchemaTableTree addChild(
            SchemaTable schemaTable,
            Direction direction,
            Class<? extends Element> elementClass,
            ReplacedStep replacedStep,
            boolean isEdgeVertexStep,
            Set<String> labels) {
        return addChild(
                schemaTable,
                direction,
                elementClass,
                replacedStep.getHasContainers(),
                replacedStep.getAndOrHasContainers(),
                replacedStep.getSqlgComparatorHolder(),
                replacedStep.getSqlgComparatorHolder().getComparators(),
                replacedStep.getSqlgRangeHolder(),
                replacedStep.getDepth(),
                isEdgeVertexStep,
                replacedStep.isEmit(),
                replacedStep.isUntilFirst(),
                replacedStep.isLeftJoin(),
                replacedStep.isDrop(),
                labels);
    }

    SchemaTableTree addChild(
            SchemaTable schemaTable,
            Direction direction,
            Class<? extends Element> elementClass,
            ReplacedStep replacedStep,
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
                replacedStep.getDepth(),
                false,
                emit,
                replacedStep.isUntilFirst(),
                replacedStep.isLeftJoin(),
                replacedStep.isDrop(),
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
            int stepDepth,
            boolean isEdgeVertexStep,
            boolean emit,
            boolean untilFirst,
            boolean leftJoin,
            boolean drop,
            Set<String> labels) {

        SchemaTableTree schemaTableTree = new SchemaTableTree(this.sqlgGraph, schemaTable, stepDepth, this.replacedStepDepth);
        if ((elementClass.isAssignableFrom(Edge.class) && schemaTable.getTable().startsWith(EDGE_PREFIX)) ||
                (elementClass.isAssignableFrom(Vertex.class) && schemaTable.getTable().startsWith(VERTEX_PREFIX))) {
            schemaTableTree.hasContainers = new ArrayList<>(hasContainers);
            schemaTableTree.andOrHasContainers = new ArrayList<>(andOrHasContainers);
            schemaTableTree.sqlgComparatorHolder = sqlgComparatorHolder;
            schemaTableTree.dbComparators = new ArrayList<>(dbComparators);
            schemaTableTree.sqlgRangeHolder = sqlgRangeHolder;
        }
        schemaTableTree.parent = this;
        schemaTableTree.direction = direction;
        this.children.add(schemaTableTree);
        schemaTableTree.stepType = isEdgeVertexStep ? STEP_TYPE.EDGE_VERTEX_STEP : STEP_TYPE.VERTEX_STEP;
        schemaTableTree.labels = Collections.unmodifiableSet(labels);
        schemaTableTree.emit = emit;
        schemaTableTree.untilFirst = untilFirst;
        schemaTableTree.optionalLeftJoin = leftJoin;
        schemaTableTree.drop = drop;
        return schemaTableTree;
    }

    private Map<String, Map<String, PropertyType>> getFilteredAllTables() {
        return getRoot().filteredAllTables;
    }

    void initializeAliasColumnNameMaps() {
        this.aliasMapHolder = new AliasMapHolder();
    }

    private Map<String, String> getColumnNameAliasMap() {
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

    public void setEmit(boolean emit) {
        this.emit = emit;
    }

    public boolean isEmit() {
        return emit;
    }

    public boolean isOptionalLeftJoin() {
        return this.optionalLeftJoin;
    }

    void setOptionalLeftJoin(boolean optionalLeftJoin) {
        this.optionalLeftJoin = optionalLeftJoin;
    }

    public void resetColumnAliasMaps() {
        this.aliasMapHolder.clear();
        this.rootAliasCounter = 1;
        this.columnListStack.clear();
    }

    public SchemaTable getSchemaTable() {
        return schemaTable;
    }

    public String constructSql(LinkedList<SchemaTableTree> distinctQueryStack) {
        Preconditions.checkState(this.parent == null, CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT);

        //If the same element occurs multiple times in the stack then the sql needs to be different.
        //This is because the same element can not be joined on more than once in sql
        //The way to overcome this is to break up the path in select sections with no duplicates and then join them together.
        if (duplicatesInStack(distinctQueryStack)) {
            List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(distinctQueryStack);
            return constructDuplicatePathSql(this.sqlgGraph, subQueryStacks);
        } else {
            //If there are no duplicates in the path then one select statement will suffice.
            return constructSinglePathSql(this.sqlgGraph, false, distinctQueryStack, null, null, false);
        }
    }

    public List<Triple<SqlgSqlExecutor.DROP_QUERY, String, SchemaTable>> constructDropSql(LinkedList<SchemaTableTree> distinctQueryStack) {
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
            //truncate logica.
            SchemaTableTree schemaTableTree = distinctQueryStack.getFirst();
            return this.sqlgGraph.getSqlDialect().sqlTruncate(this.sqlgGraph, schemaTableTree.getSchemaTable());
        } else {
            String leafNodeToDelete = constructSinglePathSql(this.sqlgGraph, false, distinctQueryStack, null, null, true);
            resetColumnAliasMaps();

            Optional<String> edgesToDelete = Optional.empty();
            if (distinctQueryStack.size() > 1 && distinctQueryStack.getLast().getSchemaTable().isVertexTable()) {
                Set<SchemaTableTree> leftJoin = new HashSet<>();
                leftJoin.add(distinctQueryStack.getLast());
                LinkedList<SchemaTableTree> edgeSchemaTableTrees = new LinkedList<>(distinctQueryStack);
                edgeSchemaTableTrees.removeLast();
                edgesToDelete = Optional.of(constructSinglePathSql(this.sqlgGraph, false, edgeSchemaTableTrees, null, null, leftJoin, true));
            }
            return this.sqlgGraph.getSqlDialect().drop(this.sqlgGraph, leafNodeToDelete, edgesToDelete, distinctQueryStack);
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

    public String constructSqlForOptional(LinkedList<SchemaTableTree> innerJoinStack, Set<SchemaTableTree> leftJoinOn) {
        Preconditions.checkState(this.parent == null, CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT);
        if (duplicatesInStack(innerJoinStack)) {
            List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(innerJoinStack);
            return constructDuplicatePathSql(this.sqlgGraph, subQueryStacks, leftJoinOn);
        } else {
            //If there are no duplicates in the path then one select statement will suffice.
            return constructSinglePathSql(this.sqlgGraph, false, innerJoinStack, null, null, leftJoinOn, false);
        }
    }

    public String constructSqlForEmit(LinkedList<SchemaTableTree> innerJoinStack) {
        Preconditions.checkState(this.parent == null, CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT);
        if (duplicatesInStack(innerJoinStack)) {
            List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(innerJoinStack);
            return constructDuplicatePathSql(this.sqlgGraph, subQueryStacks);
        } else {
            //If there are no duplicates in the path then one select statement will suffice.
            return constructSinglePathSql(this.sqlgGraph, false, innerJoinStack, null, null);
        }
    }

    public List<LinkedList<SchemaTableTree>> constructDistinctQueries() {
        Preconditions.checkState(this.parent == null, "constructDistinctQueries may only be called on the root object");
        List<LinkedList<SchemaTableTree>> result = new ArrayList<>();
        //noinspection Convert2streamapi
        for (SchemaTableTree leafNode : this.leafNodes) {
            if (leafNode.getStepDepth() == this.replacedStepDepth) {
                result.add(leafNode.constructQueryStackFromLeaf());
            }
        }
        for (LinkedList<SchemaTableTree> schemaTableTrees : result) {
            if (schemaTableTrees.get(0).getParent() != null) {
                throw new IllegalStateException("Expected root SchemaTableTree for the first SchemaTableTree in the LinkedList");
            }
        }
        return result;
    }

    public static void constructDistinctOptionalQueries(SchemaTableTree current, List<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> result) {
        LinkedList<SchemaTableTree> stack = current.constructQueryStackFromLeaf();
        //left joins but not the leave nodes as they are already present in the main sql result set.
        if (current.isOptionalLeftJoin() && (current.getStepDepth() < current.getReplacedStepDepth())) {
            Set<SchemaTableTree> leftyChildren = new HashSet<>();
            leftyChildren.addAll(current.children);
            Pair p = Pair.of(stack, leftyChildren);
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

    private String constructDuplicatePathSql(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        return constructDuplicatePathSql(sqlgGraph, subQueryLinkedLists, Collections.emptySet());
    }

    /**
     * Construct a sql statement for one original path to a leaf node.
     * As the path contains the same label more than once its been split into a List of Stacks.
     */
    private String constructDuplicatePathSql(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists, Set<SchemaTableTree> leftJoinOn) {
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
                sql = constructSinglePathSql(sqlgGraph, true, subQueryLinkedList, lastOfPrevious, null, leftJoinOn, false);
            } else {
                sql = constructSinglePathSql(sqlgGraph, true, subQueryLinkedList, lastOfPrevious, firstOfNext);
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
        singlePathSql.append(constructOuterOrderByClause(sqlgGraph, subQueryLinkedLists));
        String result = "SELECT\n\t" + constructOuterFromClause(subQueryLinkedLists);
        return result + singlePathSql;
    }

    private String constructOuterFromClause(List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        StringBuilder result = new StringBuilder();
        int countOuter = 1;
        boolean first = true;
        int i = 1;
        int columnStartIndex = 1;
        for (ColumnList columnList : this.columnListStack) {
            if (first && subQueryLinkedLists.get(i - 1).getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
                result.append("a1.").append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index")).append(" as ").append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index")).append(",\n\r");
                columnStartIndex++;
            }
            first = false;
            String from = columnList.toString("a" + countOuter++);
            result.append(from);
            if (i++ < this.columnListStack.size() && !from.isEmpty()) {
                result.append(", ");
            }
            columnStartIndex = columnList.indexColumnsExcludeForeignKey(columnStartIndex);
        }
        return result.toString();
    }

    private static String constructOuterOrderByClause(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        StringBuilder result = new StringBuilder();
        int countOuter = 1;
        // last table list with order as last step wins
        int winningOrder = 0;
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {
            if (!subQueryLinkedList.isEmpty()) {
                SchemaTableTree schemaTableTree = subQueryLinkedList.peekLast();
                if (!schemaTableTree.getDbComparators().isEmpty()) {
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
                    result.append(schemaTableTree.toOrderByClause(sqlgGraph, mutableOrderBy, countOuter));
                }
                // support range without order
                result.append(schemaTableTree.toRangeClause(sqlgGraph, mutableOrderBy));
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
                        result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                fromSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                        toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + "." + identifier + (toSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                        result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
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
                            result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    fromSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                            result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
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
                            result.append("a").append(count - 1).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                            result.append(" = a").append(count).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasIdentifier(identifier)));
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

    private String constructSinglePathSql(
            SqlgGraph sqlgGraph,
            boolean partOfDuplicateQuery,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree lastOfPrevious,
            SchemaTableTree firstOfNextStack) {
        return constructSinglePathSql(sqlgGraph, partOfDuplicateQuery, distinctQueryStack, lastOfPrevious, firstOfNextStack, Collections.emptySet(), false);
    }

    private String constructSinglePathSql(
            SqlgGraph sqlgGraph,
            boolean partOfDuplicateQuery,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree lastOfPrevious,
            SchemaTableTree firstOfNextStack,
            boolean dropStep) {
        return constructSinglePathSql(sqlgGraph, partOfDuplicateQuery, distinctQueryStack, lastOfPrevious, firstOfNextStack, Collections.emptySet(), dropStep);
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
            SqlgGraph sqlgGraph,
            boolean partOfDuplicateQuery,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree lastOfPrevious,
            SchemaTableTree firstOfNextStack,
            Set<SchemaTableTree> leftJoinOn,
            boolean dropStep) {

        return constructSelectSinglePathSql(
                sqlgGraph,
                partOfDuplicateQuery,
                distinctQueryStack,
                lastOfPrevious,
                firstOfNextStack,
                leftJoinOn,
                dropStep);
    }

    private String constructSelectSinglePathSql(
            SqlgGraph sqlgGraph,
            boolean partOfDuplicateQuery,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree lastOfPrevious,
            SchemaTableTree firstOfNextStack,
            Set<SchemaTableTree> leftJoinOn,
            boolean dropStep) {

        Preconditions.checkState(this.parent == null, "constructSelectSinglePathSql may only be called on the root SchemaTableTree");

        /*
         *columnList holds the columns per sub query.
         */
        ColumnList currentColumnList = new ColumnList(sqlgGraph, dropStep, this.getFilteredAllTables());
        this.columnListStack.add(currentColumnList);
        int startIndexColumns = 1;

        StringBuilder singlePathSql = new StringBuilder("\nSELECT\n\t");
        SchemaTableTree firstSchemaTableTree = distinctQueryStack.getFirst();
        SchemaTable firstSchemaTable = firstSchemaTableTree.getSchemaTable();

        //The SqlgVertexStep's incoming/parent element index and ids
        //dropStep must not have the index as it uses 'delete from where in (select...)' or 'WITH (SELECT) DELETE...'
        //the first column in the select must be the ID.
        //As its a DELETE there is no need for the 'index' to order on.
        if (!dropStep && lastOfPrevious == null && distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
            //if there is only 1 incoming start/traverser we use a where clause as its faster.
            if (this.parentIdsAndIndexes.size() == 1) {
                singlePathSql.append(this.parentIdsAndIndexes.get(0).getRight());
                singlePathSql.append(" as ");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
            } else if (sqlgGraph.getSqlDialect().supportsValuesExpression()) {
                //Hardcoding here for H2
                if (sqlgGraph.getSqlDialect().supportsFullValueExpression()) {
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                } else {
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("C2"));
                }
                singlePathSql.append(" as ");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
            } else {
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                singlePathSql.append(" as ");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
            }
            singlePathSql.append(",\n\t");
            //increment the ColumnList's index to take the "index" field into account.
            startIndexColumns++;
        }

        singlePathSql.append(constructFromClause(sqlgGraph, currentColumnList, distinctQueryStack, lastOfPrevious, firstOfNextStack));
        singlePathSql.append("\nFROM\n\t");
        singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getSchema()));
        singlePathSql.append(".");
        singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getTable()));
        SchemaTableTree previous = firstSchemaTableTree;
        boolean skipFirst = true;
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (skipFirst) {
                skipFirst = false;
                continue;
            }
            singlePathSql.append(constructJoinBetweenSchemaTables(sqlgGraph, previous, schemaTableTree));
            previous = schemaTableTree;
        }

        SchemaTableTree previousLeftJoinSchemaTableTree = null;
        for (SchemaTableTree schemaTableTree : leftJoinOn) {
            if (previousLeftJoinSchemaTableTree == null || !previousLeftJoinSchemaTableTree.getSchemaTable().equals(schemaTableTree.getSchemaTable())) {
                singlePathSql.append(constructJoinBetweenSchemaTables(sqlgGraph, previous, schemaTableTree, true));
            } else {
                singlePathSql.append(appendToJoinBetweenSchemaTables(sqlgGraph, previous, schemaTableTree, true));
            }
            previousLeftJoinSchemaTableTree = schemaTableTree;
        }

        //Check if there is a hasContainer with a P.within more than x.
        //If so add in a join to the temporary table that will hold the values of the P.within predicate.
        //These values are inserted/copy command into a temporary table before joining.
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (sqlgGraph.getSqlDialect().supportsBulkWithinOut() && schemaTableTree.hasBulkWithinOrOut(sqlgGraph)) {
                singlePathSql.append(schemaTableTree.bulkWithJoin(sqlgGraph));
            }
        }

        MutableBoolean mutableWhere = new MutableBoolean(false);
        MutableBoolean mutableOrderBy = new MutableBoolean(false);

        //lastOfPrevious is null for the first call in the call stack it needs the id parameter in the where clause.
        if (lastOfPrevious == null && distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
            if (this.parentIdsAndIndexes.size() != 1 && sqlgGraph.getSqlDialect().supportsValuesExpression()) {
                singlePathSql.append(" INNER JOIN\n\t(VALUES");
                int count = 1;
                for (Pair<Long, Long> parentIdAndIndex : this.parentIdsAndIndexes) {
                    singlePathSql.append("(");
                    singlePathSql.append(parentIdAndIndex.getLeft());
                    singlePathSql.append(", ");
                    singlePathSql.append(parentIdAndIndex.getRight());
                    singlePathSql.append(")");
                    if (count++ < this.parentIdsAndIndexes.size()) {
                        singlePathSql.append(",");
                    }
                }

                if (sqlgGraph.getSqlDialect().supportsFullValueExpression()) {
                    singlePathSql.append(") AS tmp (");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("tmpId"));
                    singlePathSql.append(", ");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                    singlePathSql.append(") ON ");

                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                    singlePathSql.append(".");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                    singlePathSql.append(".");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                    singlePathSql.append(" = tmp.");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("tmpId"));
                } else {
                    //This really is only for H2
                    singlePathSql.append(") ON ");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                    singlePathSql.append(".");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                    singlePathSql.append(".");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                    singlePathSql.append(" = ");
                    singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("C1"));
                }
            } else if (this.parentIdsAndIndexes.size() != 1 && !sqlgGraph.getSqlDialect().supportsValuesExpression()) {
                //Mariadb lo and behold does not support VALUES
                //Need to use a randomized name here else the temp table gets reused within the same transaction.
                SecureRandom random = new SecureRandom();
                byte bytes[] = new byte[6];
                random.nextBytes(bytes);
                String tmpTableIdentified = Base64.getEncoder().encodeToString(bytes);
                sqlgGraph.tx().normalBatchModeOn();
                for (Pair<Long, Long> parentIdsAndIndex : this.parentIdsAndIndexes) {
                    sqlgGraph.addTemporaryVertex(T.label, tmpTableIdentified, "tmpId", parentIdsAndIndex.getLeft(), "index", parentIdsAndIndex.getRight());
                }
                sqlgGraph.tx().flush();

                singlePathSql.append(" INNER JOIN\n\t");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(sqlgGraph.getSqlDialect().getPublicSchema()));
                singlePathSql.append(".");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + tmpTableIdentified));
                singlePathSql.append(" as tmp");
                singlePathSql.append(" ON ");

                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                singlePathSql.append(".");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                singlePathSql.append(".");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                singlePathSql.append(" = tmp.");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("tmpId"));
            } else {
                singlePathSql.append("\nWHERE\n\t");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()));
                singlePathSql.append(".");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()));
                singlePathSql.append(".");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                singlePathSql.append(" = ");
                singlePathSql.append(this.parentIdsAndIndexes.get(0).getLeft());
                mutableWhere.setTrue();
            }
        }

        //construct the where clause for the hasContainers
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            singlePathSql.append(schemaTableTree.toWhereClause(sqlgGraph, mutableWhere));
        }
        //add in the is null where clause for the optional left joins
        for (SchemaTableTree schemaTableTree : leftJoinOn) {
            singlePathSql.append(schemaTableTree.toOptionalLeftJoinWhereClause(sqlgGraph, mutableWhere));
        }

        //if partOfDuplicateQuery then the order by clause is on the outer select
        if (!partOfDuplicateQuery) {

            if (!dropStep && lastOfPrevious == null && distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
                singlePathSql.append("\nORDER BY\n\t");
                singlePathSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("index"));
                mutableOrderBy.setTrue();
            }

            //construct the order by clause for the comparators
            for (SchemaTableTree schemaTableTree : distinctQueryStack) {
                singlePathSql.append(schemaTableTree.toOrderByClause(sqlgGraph, mutableOrderBy, -1));
                singlePathSql.append(schemaTableTree.toRangeClause(sqlgGraph, mutableOrderBy));
            }
        }

        currentColumnList.indexColumns(startIndexColumns);
        return singlePathSql.toString();
    }


    private boolean hasBulkWithinOrOut(SqlgGraph sqlgGraph) {
        return this.hasContainers.stream().anyMatch(h -> SqlgUtil.isBulkWithinAndOut(sqlgGraph, h));
    }

    private String bulkWithJoin(SqlgGraph sqlgGraph) {
        StringBuilder sb = new StringBuilder();
        List<HasContainer> bulkHasContainers = this.hasContainers.stream().filter(h -> SqlgUtil.isBulkWithinAndOut(sqlgGraph, h)).collect(Collectors.toList());
        for (HasContainer hasContainer : bulkHasContainers) {
            P<List<Object>> predicate = (P<List<Object>>) hasContainer.getPredicate();
            Collection<Object> withInList = predicate.getValue();
            Set<Object> withInOuts = new HashSet<>(withInList);

            Map<String, PropertyType> columns = new HashMap<>();
            Object next = withInOuts.iterator().next();
            if (next instanceof RecordId) {
                next = ((RecordId) next).getId();
            }
            if (hasContainer.getBiPredicate() == Contains.within) {
                columns.put(WITHIN, PropertyType.from(next));
            } else if (hasContainer.getBiPredicate() == Contains.without) {
                columns.put(WITHOUT, PropertyType.from(next));
            } else {
                throw new UnsupportedOperationException("Only Contains.within and Contains.without is supported!");
            }

            if (hasContainer.getBiPredicate() == Contains.within) {
                sb.append(" INNER JOIN\n\t");
            } else {
                //left join and in the where clause add a IS NULL, to find the values not in the right hand table
                sb.append(" LEFT JOIN\n\t");
            }
            sb.append("(VALUES ");
            boolean first = true;
            for (Object withInOutValue : withInOuts) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                if (withInOutValue instanceof RecordId) {
                    withInOutValue = ((RecordId) withInOutValue).getId();
                }
                sb.append("(");
                PropertyType propertyType = PropertyType.from(withInOutValue);
                sb.append(sqlgGraph.getSqlDialect().valueToValuesString(propertyType, withInOutValue));
                sb.append(")");
            }
            sb.append(") as tmp");
            sb.append(this.rootSchemaTableTree().tmpTableAliasCounter);
            sb.append("(");
            if (hasContainer.getBiPredicate() == Contains.within) {
                sb.append(WITHIN);
            } else {
                sb.append(WITHOUT);
            }
            sb.append(") ");
            sb.append(" on ");
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

        }
        return sb.toString();
    }

    private String toOptionalLeftJoinWhereClause(SqlgGraph sqlgGraph, MutableBoolean printedWhere) {
        final StringBuilder result = new StringBuilder();
        if (!printedWhere.booleanValue()) {
            printedWhere.setTrue();
            result.append("\nWHERE\n\t(");
        } else {
            result.append(" AND\n\t(");
        }
        if (this.drop) {
            Preconditions.checkState(this.parent.getSchemaTable().isEdgeTable(), "Optional left join drop queries must be for an edge!");
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
            result.append(".");
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
            result.append(".");
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
            result.append(" IS NULL) ");
            result.append("AND\n\t(");
            String rawLabel = this.getSchemaTable().getTable().substring(EDGE_PREFIX.length());
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parent.getSchemaTable().getSchema()));
            result.append(".");
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parent.getSchemaTable().getTable()));
            result.append(".");
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                    this.getSchemaTable().getSchema() + "." + rawLabel +
                            (this.getDirection() == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
            result.append(" IS NOT NULL)");
        } else {
            Preconditions.checkState(this.parent.getSchemaTable().isVertexTable(), "Optional left join non drop queries must be for an vertex!");
            String rawLabel = this.parent.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
            result.append(".");
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
            result.append(".");
            result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                    this.parent.getSchemaTable().getSchema() + "." + rawLabel +
                            (this.getDirection() == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
            result.append(" IS NULL)");
        }
        return result.toString();
    }

    private String toWhereClause(SqlgGraph sqlgGraph, MutableBoolean printedWhere) {
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
                if (sqlgGraph.getSqlDialect().isMariaDb()) {
                    result.append(" COLLATE latin1_general_cs");
                }
            }
        }
        for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
            if (!printedWhere.booleanValue()) {
                printedWhere.setTrue();
                result.append("\nWHERE ");
            } else {
                result.append(" AND ");
            }
            andOrHasContainer.toSql(sqlgGraph, this, result);
        }
        return result.toString();
    }

    private String toOrderByClause(SqlgGraph sqlgGraph, MutableBoolean printedOrderBy, int counter) {
        String result = "";
        for (org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>> comparator : this.getDbComparators()) {
            if (!printedOrderBy.booleanValue()) {
                printedOrderBy.setTrue();
                result += "\nORDER BY\n\t";
            } else {
                result += ",\n\t";
            }
            if (comparator.getValue1() instanceof ElementValueComparator) {
                ElementValueComparator<?> elementValueComparator = (ElementValueComparator<?>) comparator.getValue1();
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
                result += " " + alias;
                if (elementValueComparator.getValueComparator() == Order.incr) {
                    result += " ASC";
                } else if (elementValueComparator.getValueComparator() == Order.decr) {
                    result += " DESC";
                } else {
                    throw new RuntimeException("Only handle Order.incr and Order.decr, not " + elementValueComparator.getValueComparator().toString());
                }

                //TODO redo this via SqlgOrderGlobalStep
            } else if ((comparator.getValue0() instanceof ElementValueTraversal<?> || comparator.getValue0() instanceof TokenTraversal<?, ?>)
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
                if (t instanceof ElementValueTraversal) {
                    ElementValueTraversal<?> elementValueTraversal = (ElementValueTraversal<?>) t;
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
                result += " " + alias;
                if (comparator.getValue1() == Order.incr) {
                    result += " ASC";
                } else if (comparator.getValue1() == Order.decr) {
                    result += " DESC";
                } else {
                    throw new RuntimeException("Only handle Order.incr and Order.decr, not " + comparator.getValue1().toString());
                }
            } else {
                Preconditions.checkState(comparator.getValue0().getSteps().size() == 1, "toOrderByClause expects a TraversalComparator to have exactly one step!");
                Preconditions.checkState(comparator.getValue0().getSteps().get(0) instanceof SelectOneStep, "toOrderByClause expects a TraversalComparator to have exactly one SelectOneStep!");
                SelectOneStep selectOneStep = (SelectOneStep) comparator.getValue0().getSteps().get(0);
                Preconditions.checkState(selectOneStep.getScopeKeys().size() == 1, "toOrderByClause expects the selectOneStep to have one scopeKey!");
                Preconditions.checkState(selectOneStep.getLocalChildren().size() == 1, "toOrderByClause expects the selectOneStep to have one traversal!");
                Traversal.Admin<?, ?> t = (Traversal.Admin<?, ?>) selectOneStep.getLocalChildren().get(0);
                Preconditions.checkState(
                        t instanceof ElementValueTraversal ||
                                t instanceof TokenTraversal,
                        "toOrderByClause expects the selectOneStep's traversal to be a ElementValueTraversal or TokenTraversal!");

                //need to find the schemaTable that the select is for.
                //this schemaTable is for the leaf node as the order by only occurs last in gremlin (optimized gremlin that is)
                String select = (String) selectOneStep.getScopeKeys().iterator().next();
                SchemaTableTree selectSchemaTableTree = findSelectSchemaTable(select);
                Preconditions.checkState(selectSchemaTableTree != null, "SchemaTableTree not found for " + select);

                String prefix;
                if (selectSchemaTableTree.children.isEmpty()) {
                    //counter is -1 for single queries, i.e. they are not prefixed with ax
                    prefix = String.valueOf(selectSchemaTableTree.stepDepth);
                    prefix += SchemaTableTree.ALIAS_SEPARATOR;
                } else {
                    prefix = String.valueOf(selectSchemaTableTree.stepDepth);
                    prefix += SchemaTableTree.ALIAS_SEPARATOR;
                    prefix += selectSchemaTableTree.labels.iterator().next();
                    prefix += SchemaTableTree.ALIAS_SEPARATOR;
                }
                prefix += selectSchemaTableTree.getSchemaTable().getSchema();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += selectSchemaTableTree.getSchemaTable().getTable();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                if (t instanceof ElementValueTraversal) {
                    ElementValueTraversal<?> elementValueTraversal = (ElementValueTraversal<?>) t;
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
                result += " " + alias;
                if (comparator.getValue1() == Order.incr) {
                    result += " ASC";
                } else if (comparator.getValue1() == Order.decr) {
                    result += " DESC";
                } else {
                    throw new RuntimeException("Only handle Order.incr and Order.decr, not " + comparator.toString());
                }
            }
        }
        return result;
    }

    private String toRangeClause(SqlgGraph sqlgGraph, MutableBoolean mutableOrderBy) {
        if (this.sqlgRangeHolder != null && this.sqlgRangeHolder.isApplyOnDb()) {
            if (this.sqlgRangeHolder.hasRange()) {
                //This is MssqlServer, ugly but what to do???
                String sql = "";
                if (mutableOrderBy.isFalse() && sqlgGraph.getSqlDialect().isMssqlServer() && this.getDbComparators().isEmpty()) {
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

    /**
     * Checks if the stack has the same element more than once.
     *
     * @param distinctQueryStack
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
     * Constructs the from clause with the required selected fields needed to make the join between the previous and the next SchemaTable
     *
     * @param columnList
     * @param distinctQueryStack      //     * @param firstSchemaTableTree    This is the first SchemaTable in the current sql stack. If it is an Edge table then its foreign key
     *                                //     *                                field to the previous table need to be in the select clause in order for the join statement to
     *                                //     *                                reference it.
     *                                //     * @param lastSchemaTableTree
     * @param previousSchemaTableTree The previous schemaTableTree that will be joined to.
     * @param nextSchemaTableTree     represents the table to join to. it is null for the last table as there is nothing to join to.  @return
     */
    private static String constructFromClause(
            SqlgGraph sqlgGraph,
            ColumnList columnList,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree previousSchemaTableTree,
            SchemaTableTree nextSchemaTableTree) {

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

        boolean printedId = false;

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
                        columnList.add(firstSchemaTable,
                                previousSchemaTableTree.getSchemaTable().getSchema() + "." + previousRawLabel + "." + identifier + Topology.OUT_VERTEX_COLUMN_END,
                                previousSchemaTableTree.stepDepth,
                                firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction, identifier));
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
                        columnList.add(firstSchemaTable,
                                previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        previousRawLabel + "." + identifier + Topology.IN_VERTEX_COLUMN_END,
                                previousSchemaTableTree.stepDepth,
                                firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction, identifier));
                    }
                }
            }
        } else if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            //if user defined identifiers then the regular properties make up the ids.
            if (firstSchemaTableTree.hasIDPrimaryKey) {
                columnList.add(firstSchemaTable, Topology.ID, firstSchemaTableTree.stepDepth, firstSchemaTableTree.calculatedAliasId());
            }
            printedId = firstSchemaTable == lastSchemaTable;
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
                        columnList.add(lastSchemaTable,
                                nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        nextRawLabel + "." + identifier + (nextSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END),
                                nextSchemaTableTree.stepDepth,
                                lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                        nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        nextRawLabel + "." + identifier + (nextSchemaTableTree.direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END));
                    }

                }
                constructAllLabeledFromClause(distinctQueryStack, columnList);
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
                        columnList.add(lastSchemaTable,
                                nextSchemaTableTree.getSchemaTable().getSchema() + "." + nextRawLabel + "." + identifier + (nextSchemaTableTree.direction == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END),
                                nextSchemaTableTree.stepDepth,
                                lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                        nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        nextRawLabel + "." + identifier + (nextSchemaTableTree.direction == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END));

                    }
                }
                constructAllLabeledFromClause(distinctQueryStack, columnList);
                constructEmitEdgeIdFromClause(distinctQueryStack, columnList);
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
            constructAllLabeledFromClause(distinctQueryStack, columnList);
            printedId = firstSchemaTable == lastSchemaTable;
        }

        //The last schemaTableTree in the call stack has no nextSchemaTableTree.
        //This last element's properties need to be returned, including all labeled properties for this path
        if (nextSchemaTableTree == null) {
            if (!printedId && lastSchemaTableTree.hasIDPrimaryKey) {
                printIDFromClauseFor(lastSchemaTableTree, columnList);
            }
            printFromClauseFor(lastSchemaTableTree, columnList);

            if (lastSchemaTableTree.getSchemaTable().isEdgeTable()) {
                printEdgeInOutVertexIdFromClauseFor(sqlgGraph, firstSchemaTableTree, lastSchemaTableTree, columnList);
            }

            constructAllLabeledFromClause(distinctQueryStack, columnList);
            constructEmitFromClause(distinctQueryStack, columnList);
        }
        return columnList.toString();
    }

    private static void constructAllLabeledFromClause(LinkedList<SchemaTableTree> distinctQueryStack, ColumnList cols) {
        List<SchemaTableTree> labeled = distinctQueryStack.stream().filter(d -> !d.getLabels().isEmpty()).collect(Collectors.toList());
        for (SchemaTableTree schemaTableTree : labeled) {
            if (schemaTableTree.hasIDPrimaryKey) {
                printLabeledIDFromClauseFor(schemaTableTree, cols);
            }
            printLabeledFromClauseFor(schemaTableTree, cols);
            if (schemaTableTree.getSchemaTable().isEdgeTable()) {
                schemaTableTree.printLabeledEdgeInOutVertexIdFromClauseFor(cols);
            }
        }
    }

    private static void constructEmitEdgeIdFromClause(LinkedList<SchemaTableTree> distinctQueryStack, ColumnList cols) {
        List<SchemaTableTree> emitted = distinctQueryStack.stream()
                .filter(d -> d.getSchemaTable().isEdgeTable() && d.isEmit())
                .collect(Collectors.toList());
        for (SchemaTableTree schemaTableTree : emitted) {
            printEdgeId(schemaTableTree, cols);
        }
    }

    /**
     * If emit is true then the edge id also needs to be printed.
     * This is required when there are multiple edges to the same vertex.
     * Only by having access to the edge id can on tell if the vertex needs to be emitted.
     */
    private static void constructEmitFromClause(LinkedList<SchemaTableTree> distinctQueryStack, ColumnList cols) {
        int count = 1;
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (count > 1) {
                if (!schemaTableTree.getSchemaTable().isEdgeTable() && schemaTableTree.isEmit()) {
                    //if the VertexStep is for an edge table there is no need to print edge ids as its already printed.
                    printEdgeId(schemaTableTree.parent, cols);
                }
            }
            count++;
        }
    }

    private static void printEdgeId(SchemaTableTree schemaTableTree, ColumnList cols) {
        Preconditions.checkArgument(schemaTableTree.getSchemaTable().isEdgeTable());
        cols.add(schemaTableTree, Topology.ID, schemaTableTree.calculatedAliasId());
    }

    private static void printIDFromClauseFor(SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        cols.add(lastSchemaTableTree, Topology.ID, lastSchemaTableTree.calculatedAliasId());
    }

    private static void printFromClauseFor(SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        Map<String, PropertyType> propertyTypeMap = lastSchemaTableTree.getFilteredAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
        ListOrderedSet<String> identifiers = lastSchemaTableTree.getIdentifiers();
        for (String identifier : identifiers) {
            PropertyType propertyType = propertyTypeMap.get(identifier);
            String alias = lastSchemaTableTree.calculateAliasPropertyName(identifier);
            cols.add(lastSchemaTableTree, identifier, alias);
            for (String postFix : propertyType.getPostFixes()) {
                alias = lastSchemaTableTree.calculateAliasPropertyName(identifier + postFix);
                cols.add(lastSchemaTableTree, identifier + postFix, alias);
            }
        }
        for (Map.Entry<String, PropertyType> propertyTypeMapEntry : propertyTypeMap.entrySet()) {
            if (!identifiers.contains(propertyTypeMapEntry.getKey())) {
                String alias = lastSchemaTableTree.calculateAliasPropertyName(propertyTypeMapEntry.getKey());
                cols.add(lastSchemaTableTree, propertyTypeMapEntry.getKey(), alias);
                for (String postFix : propertyTypeMapEntry.getValue().getPostFixes()) {
                    alias = lastSchemaTableTree.calculateAliasPropertyName(propertyTypeMapEntry.getKey() + postFix);
                    cols.add(lastSchemaTableTree, propertyTypeMapEntry.getKey() + postFix, alias);
                }
            }
        }
    }

    private static void printLabeledIDFromClauseFor(SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        String alias = cols.getAlias(lastSchemaTableTree, Topology.ID);
        if (alias == null) {
            alias = lastSchemaTableTree.calculateLabeledAliasId();
            cols.add(lastSchemaTableTree, Topology.ID, alias);
        } else {
            lastSchemaTableTree.calculateLabeledAliasId(alias);
        }

    }

    private static void printLabeledFromClauseFor(SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        Map<String, PropertyType> propertyTypeMap = lastSchemaTableTree.getFilteredAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
        for (Map.Entry<String, PropertyType> propertyTypeMapEntry : propertyTypeMap.entrySet()) {
            String col = propertyTypeMapEntry.getKey();
            String alias = cols.getAlias(lastSchemaTableTree, col);
            if (alias == null) {
                alias = lastSchemaTableTree.calculateLabeledAliasPropertyName(propertyTypeMapEntry.getKey());
                cols.add(lastSchemaTableTree, col, alias);
            } else {
                lastSchemaTableTree.calculateLabeledAliasPropertyName(propertyTypeMapEntry.getKey(), alias);
            }
            for (String postFix : propertyTypeMapEntry.getValue().getPostFixes()) {
                col = propertyTypeMapEntry.getKey() + postFix;
                alias = cols.getAlias(lastSchemaTableTree, col);
                // postfix do not use labeled methods
                if (alias == null) {
                    alias = lastSchemaTableTree.calculateAliasPropertyName(propertyTypeMapEntry.getKey() + postFix);
                    cols.add(lastSchemaTableTree, col, alias);
                }
            }
        }
    }

    private static void printEdgeInOutVertexIdFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree firstSchemaTableTree, SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        Preconditions.checkState(lastSchemaTableTree.getSchemaTable().isEdgeTable());
        Set<ForeignKey> edgeForeignKeys = sqlgGraph.getTopology().getAllEdgeForeignKeys().get(lastSchemaTableTree.getSchemaTable().toString());
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
        Set<ForeignKey> edgeForeignKeys = this.sqlgGraph.getTopology().getAllEdgeForeignKeys().get(this.getSchemaTable().toString());
        for (ForeignKey edgeForeignKey : edgeForeignKeys) {
            for (String foreignKey : edgeForeignKey.getCompositeKeys()) {
                String alias = cols.getAlias(this.getSchemaTable(), foreignKey, this.stepDepth);
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
                //This must be a dot as its the foreign key column, i.e. blah__I
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

    private String lastMappedAliasIdentifier(String identifier) {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + identifier;
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
            this.reducedLabels = getLabels().stream().reduce((a, b) -> a + ALIAS_SEPARATOR + b).get();
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

    private static String constructJoinBetweenSchemaTables(SqlgGraph sqlgGraph, SchemaTableTree fromSchemaTableTree, SchemaTableTree labelToTraversTree) {
        return constructJoinBetweenSchemaTables(sqlgGraph, fromSchemaTableTree, labelToTraversTree, false);
    }

    private static String constructJoinBetweenSchemaTables(SqlgGraph sqlgGraph, SchemaTableTree fromSchemaTableTree, SchemaTableTree labelToTraversTree, boolean leftJoin) {
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
            joinSql = new StringBuilder(" INNER JOIN\n\t");
        }
        if (fromSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
            joinSql.append(".");
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
            joinSql.append(" ON ");
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema()));
            joinSql.append(".");
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable()));
            joinSql.append(".");
            if (fromSchemaTableTree.isHasIDPrimaryKey()) {
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
        } else {
            //From edge to vertex table the foreign key is opposite to the direction.
            //This is because this is second part of the traversal via the edge.
            //This code did not take specific traversals from the edge into account.

            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema()));
            joinSql.append(".");
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable()));
            joinSql.append(" ON ");
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getSchema()));
            joinSql.append(".");
            joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTable.getTable()));
            joinSql.append(".");
            if (labelToTraversTree.isHasIDPrimaryKey()) {
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

                    if (labelToTraversTree.isEdgeVertexStep()) {
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                                rawLabelToTravers + "." + identifier + (labelToTraversTree.getDirection() == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                    } else {
                        joinSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                                rawLabelToTravers + "." + identifier + (labelToTraversTree.getDirection() == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
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

    private static String appendToJoinBetweenSchemaTables(SqlgGraph sqlgGraph, SchemaTableTree fromSchemaTableTree, SchemaTableTree labelToTraversTree, boolean leftJoin) {
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
                removeObsoleteHasContainers(current);
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
     *
     * @param schemaTableTree the current table tree
     */
    private void removeObsoleteHasContainers(final SchemaTableTree schemaTableTree) {
        Set<HasContainer> toRemove = new HashSet<>();
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
        }
        schemaTableTree.hasContainers.removeAll(toRemove);
    }

    private SchemaTable getHasContainerSchemaTable(SchemaTableTree schemaTableTree, SchemaTable predicateSchemaTable) {
        SchemaTable hasContainerLabelSchemaTable;
        //Check if we are on a vertex or edge
        if (schemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)) {
            hasContainerLabelSchemaTable = SchemaTable.of(predicateSchemaTable.getSchema(), VERTEX_PREFIX + predicateSchemaTable.getTable());
        } else {
            hasContainerLabelSchemaTable = SchemaTable.of(predicateSchemaTable.getSchema(), EDGE_PREFIX + predicateSchemaTable.getTable());
        }
        return hasContainerLabelSchemaTable;
    }

    private SchemaTable getIDContainerSchemaTable(SchemaTableTree schemaTableTree, Object value) {
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
     * @param schemaTableTree
     * @return true if any has container does NOT match, false if everything is fine
     */
    private boolean invalidateByHas(SchemaTableTree schemaTableTree) {
        for (HasContainer hasContainer : schemaTableTree.hasContainers) {
            if (!hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_WITHOUT) && !hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_FROM)) {
                if (hasContainer.getKey().equals(label.getAccessor())) {
                    Preconditions.checkState(false, "label hasContainers should have been removed by now.");
                } else if (hasContainer.getKey().equals(T.id.getAccessor())) {
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
                    if (!this.getFilteredAllTables().get(schemaTableTree.getSchemaTable().toString()).containsKey(hasContainer.getKey())) {
                        if (!Existence.NULL.equals(hasContainer.getBiPredicate())) {
                            return true;
                        }
                    }
                    //Check if it is a Contains.within with a empty list of values
                    if (hasEmptyWithin(hasContainer)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private boolean hasEmptyWithin(HasContainer hasContainer) {
        if (hasContainer.getBiPredicate() == Contains.within) {
            return ((Collection) hasContainer.getPredicate().getValue()).isEmpty();
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return this.schemaTable.toString();
    }

    public String toTreeString() {
        StringBuilder result = new StringBuilder();
        internalToString(result);
        return result.toString();
    }

    private void internalToString(StringBuilder sb) {
        if (sb.length() > 0) {
            sb.append("\n");
        }
        for (int i = 0; i < this.stepDepth; i++) {
            sb.append("\t");
        }
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

    public List<AndOrHasContainer> getAndOrHasContainers() {
        return andOrHasContainers;
    }

    public SqlgComparatorHolder getSqlgComparatorHolder() {
        return this.sqlgComparatorHolder;
    }

    public List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> getDbComparators() {
        return this.dbComparators;
    }

    public int getStepDepth() {
        return stepDepth;
    }

    public int getReplacedStepDepth() {
        return replacedStepDepth;
    }

    @Override
    public int hashCode() {
        if (this.parent != null) {
            if (this.direction == null) {
                return (this.schemaTable.toString() + this.parent.toString()).hashCode();
            } else {
                return (this.schemaTable.toString() + this.direction.name() + this.parent.toString()).hashCode();
            }
        } else {
            if (this.direction == null) {
                return this.schemaTable.toString().hashCode();
            } else {
                return (this.schemaTable.toString() + this.direction.name()).hashCode();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof SchemaTableTree)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        SchemaTableTree other = (SchemaTableTree) o;
        if (this.direction != other.direction) {
            return false;
        } else if (this.parent != null && other.parent == null) {
            return false;
        } else if (this.parent == null && other.parent != null) {
            return false;
        } else if (this.parent == null && other.parent == null) {
            return this.schemaTable.equals(other.parent);
        } else {
            return this.parent.equals(other.parent) && this.schemaTable.equals(other.schemaTable);
        }
    }

    public Set<String> getLabels() {
        return this.labels;
    }

    public Set<String> getRealLabels() {
        if (this.realLabels == null) {
            this.realLabels = new HashSet<>();
            for (String label : this.labels) {
                if (label.contains(BaseStrategy.PATH_LABEL_SUFFIX)) {
                    this.realLabels.add(label.substring(label.indexOf(BaseStrategy.PATH_LABEL_SUFFIX) + BaseStrategy.PATH_LABEL_SUFFIX.length()));
                } else if (label.contains(BaseStrategy.EMIT_LABEL_SUFFIX)) {
                    this.realLabels.add(label.substring(label.indexOf(BaseStrategy.EMIT_LABEL_SUFFIX) + BaseStrategy.EMIT_LABEL_SUFFIX.length()));
                } else {
                    throw new IllegalStateException("label must contain " + BaseStrategy.PATH_LABEL_SUFFIX + " or " + BaseStrategy.EMIT_LABEL_SUFFIX);
                }
            }
        }
        return this.realLabels;
    }

    private boolean isEdgeVertexStep() {
        return this.stepType == STEP_TYPE.EDGE_VERTEX_STEP;
    }

    private boolean isVertexStep() {
        return this.stepType == STEP_TYPE.VERTEX_STEP;
    }

    void setStepType(STEP_TYPE stepType) {
        this.stepType = stepType;
    }

    public boolean isUntilFirst() {
        return untilFirst;
    }

    void setUntilFirst(boolean untilFirst) {
        this.untilFirst = untilFirst;
    }

    int getTmpTableAliasCounter() {
        return tmpTableAliasCounter;
    }

    public void loadProperty(ResultSet resultSet, SqlgElement sqlgElement) throws SQLException {
        for (ColumnList columnList : this.getColumnListStack()) {
            LinkedHashMap<ColumnList.Column, String> columns = columnList.getFor(this.stepDepth, this.schemaTable);
            for (ColumnList.Column column : columns.keySet()) {
                if (!column.getColumn().equals("index")) {
                    String propertyName = column.getColumn();
                    PropertyType propertyType = column.getPropertyType();
                    if (!column.isID() && !column.isForeignKey()) {
                        sqlgElement.loadProperty(resultSet, propertyName, column.getColumnIndex(), getColumnNameAliasMap(), this.stepDepth, propertyType);
                    }
                }
            }
        }
    }

    public void loadEdgeInOutVertices(ResultSet resultSet, SqlgEdge sqlgEdge) throws SQLException {
        Preconditions.checkState(this.schemaTable.isEdgeTable());
        for (ColumnList columnList : this.getColumnListStack()) {
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

    public ListOrderedSet<Object> loadIdentifierObjects(Map<String, Integer> idColumnCountMap, ResultSet resultSet) throws SQLException {
        ListOrderedSet<Object> identifierObjects = new ListOrderedSet<>();
        for (String identifier : this.identifiers) {
            String labelledAliasIdentifier = labeledAliasIdentifier(identifier);
            int count = idColumnCountMap.get(labelledAliasIdentifier);
            identifierObjects.add(resultSet.getObject(count));
        }
        return identifierObjects;
    }

    public void clearColumnNamePropertyNameMap() {
        if (this.columnNamePropertyName != null) {
            this.columnNamePropertyName.clear();
            this.columnNamePropertyName = null;
        }
    }

    public String idProperty() {
        if (this.idProperty == null) {
            this.idProperty = this.stepDepth + ALIAS_SEPARATOR + schemaTable.getSchema() + SchemaTableTree.ALIAS_SEPARATOR + schemaTable.getTable() + SchemaTableTree.ALIAS_SEPARATOR + Topology.ID;
        }
        return this.idProperty;
    }

    public boolean isLocalStep() {
        return localStep;
    }

    void setLocalStep(boolean localStep) {
        this.localStep = localStep;
    }

    public boolean isFakeEmit() {
        return fakeEmit;
    }

    public void setFakeEmit(boolean fakeEmit) {
        this.fakeEmit = fakeEmit;
    }

    public void setParentIdsAndIndexes(List<Pair<Long, Long>> parentIdsAndIndexes) {
        this.parentIdsAndIndexes = parentIdsAndIndexes;
    }

    public STEP_TYPE getStepType() {
        return this.stepType;
    }

    public List<Pair<Long, Long>> getParentIdsAndIndexes() {
        return this.parentIdsAndIndexes;
    }

    public void removeDbComparators() {
        this.dbComparators = new ArrayList<>();
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
        return identifiers;
    }

    private List<ColumnList> getColumnListStack() {
        return this.getRoot().columnListStack;
    }

}
