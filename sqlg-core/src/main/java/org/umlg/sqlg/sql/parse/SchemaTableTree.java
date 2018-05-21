package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
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
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
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
    private List<HasContainer> hasContainers = new ArrayList<>();
    private List<AndOrHasContainer> andOrHasContainers = new ArrayList<>();
    private SqlgComparatorHolder sqlgComparatorHolder = new SqlgComparatorHolder();
    private List<org.javatuples.Pair<Traversal.Admin<?, ?>, Comparator<?>>> dbComparators = new ArrayList<>();
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
        initializeAliasColumnNameMaps();
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

    private Map<String, Pair<String, PropertyType>> getColumnNamePropertyName() {
        if (this.columnNamePropertyName == null) {
            this.columnNamePropertyName = new HashMap<>();
            for (Map.Entry<String, String> entry : getRoot().aliasMapHolder.getAliasColumnNameMap().entrySet()) {
                String alias = entry.getKey();
                String columnName = entry.getValue();
                //only load the labelled columns
                if (!columnName.endsWith(SchemaTableTree.ALIAS_SEPARATOR + Topology.ID) &&
                        (columnName.contains(BaseStrategy.PATH_LABEL_SUFFIX) || columnName.contains(BaseStrategy.EMIT_LABEL_SUFFIX))) {

                    if (containsLabelledColumn(columnName)) {
                        String propertyName = propertyNameFromLabeledAlias(columnName);
                        PropertyType propertyType = this.sqlgGraph.getTopology().getTableFor(getSchemaTable()).get(propertyName);
                        this.columnNamePropertyName.put(alias, Pair.of(propertyName, propertyType));
                    }
                }
            }
        }
        return this.columnNamePropertyName;
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

    public void setOptionalLeftJoin(boolean optionalLeftJoin) {
        this.optionalLeftJoin = optionalLeftJoin;
    }

    public void resetColumnAliasMaps() {
        this.aliasMapHolder.clear();
        this.rootAliasCounter = 1;
    }

    private boolean containsLabelledColumn(String columnName) {
        if (columnName.startsWith(this.stepDepth + ALIAS_SEPARATOR + this.reducedLabels() + ALIAS_SEPARATOR)) {
            String column = columnName.substring((this.stepDepth + ALIAS_SEPARATOR + this.reducedLabels() + ALIAS_SEPARATOR).length());
            Iterator<String> split = Splitter.on(ALIAS_SEPARATOR).split(column).iterator();
            String schema = split.next();
            String table = split.next();
            return schema.equals(this.schemaTable.getSchema()) && table.equals(this.schemaTable.getTable());
        } else {
            return false;
        }
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
        String singlePathSql = "\nFROM (";
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
            singlePathSql += sql;
            if (count == 1) {
                singlePathSql += "\n) a" + count++ + " INNER JOIN (";
            } else {
                //join the last with the first
                singlePathSql += "\n) a" + count + " ON ";
                singlePathSql += constructSectionedJoin(sqlgGraph, lastOfPrevious, firstSchemaTableTree, count);
                if (count++ < subQueryLinkedLists.size()) {
                    singlePathSql += " INNER JOIN (";
                }
            }
            lastOfPrevious = subQueryLinkedList.getLast();
        }
        singlePathSql += constructOuterOrderByClause(sqlgGraph, subQueryLinkedLists);
        String result = "SELECT\n\t" + constructOuterFromClause(subQueryLinkedLists);
        return result + singlePathSql;
    }

    private String constructOuterFromClause(List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        String result = "";
        int countOuter = 1;
        LinkedList<SchemaTableTree> previousSubQuery = null;
        boolean first = true;
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {

            int countInner = 1;
            for (SchemaTableTree schemaTableTree : subQueryLinkedList) {

                //print the ID of the incoming element.
                if (first && subQueryLinkedList.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
                    result += "a1." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index") + " as " +
                            this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("index") + ",\n\r";
                }
                first = false;

                //labelled entries need to be in the outer select
                if (!schemaTableTree.getLabels().isEmpty()) {
                    result = schemaTableTree.printLabeledOuterFromClause(result, countOuter, schemaTableTree.getColumnNameAliasMap());
                    result += ", ";
                }
                if (schemaTableTree.getSchemaTable().isEdgeTable() && schemaTableTree.isEmit()) {
                    Optional<String> optional = schemaTableTree.printEmitMappedAliasIdForOuterFromClause(countOuter, schemaTableTree.getColumnNameAliasMap());
                    if (optional.isPresent()) {
                        result += optional.get();
                        result += ", ";
                    }
                }
                //last entry, always print this
                if (countOuter == subQueryLinkedLists.size() && countInner == subQueryLinkedList.size()) {
                    @SuppressWarnings("ConstantConditions")
                    SchemaTableTree previousSchemaTableTree = previousSubQuery.getLast();
                    result += schemaTableTree.printOuterFromClause(countOuter, schemaTableTree.getColumnNameAliasMap(), previousSchemaTableTree);
                    result += ", ";
                }
                countInner++;

            }
            previousSubQuery = subQueryLinkedList;
            countOuter++;
        }
        result = result.substring(0, result.length() - 2);
        return result;
    }

    private Optional<String> printEmitMappedAliasIdForOuterFromClause(int countOuter, Map<String, String> columnNameAliasMap) {
        Optional<String> optional = this.mappedAliasIdForOuterFromClause(columnNameAliasMap);
        if (optional.isPresent()) {
            return Optional.of(" a" + countOuter + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(optional.get()));
        } else {
            return Optional.empty();
        }
    }

    private static String constructOuterOrderByClause(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        String result = "";
        int countOuter = 1;
        // last table list with order as last step wins
        int winningOrder = 0;
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {
            if (!subQueryLinkedList.isEmpty()) {
                SchemaTableTree schemaTableTree = subQueryLinkedList.peekLast();
                if (!schemaTableTree.getDbComparators().isEmpty()
                        ) {
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
                    result += schemaTableTree.toOrderByClause(sqlgGraph, mutableOrderBy, countOuter);
                }
                // support range without order
                result += schemaTableTree.toRangeClause(sqlgGraph, mutableOrderBy);
            }
            countOuter++;
        }
        return result;
    }

    private String printOuterFromClause(int count, Map<String, String> columnNameAliasMapCopy, SchemaTableTree previousSchemaTableTree) {
        String sql = "";
        Map<String, PropertyType> propertyTypeMap = this.getFilteredAllTables().get(this.toString());
        Optional<String> optional = this.lastMappedAliasIdForOuterFrom(columnNameAliasMapCopy);
        if (optional.isPresent()) {
            sql = "a" + count + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(optional.get());
            if (propertyTypeMap.size() > 0) {
                sql += ", ";
            }
        }
        int propertyCount = 1;
        for (Map.Entry<String, PropertyType> propertyNameEntry : propertyTypeMap.entrySet()) {
            sql += "a" + count + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.mappedAliasPropertyName(propertyNameEntry.getKey(), columnNameAliasMapCopy));
            for (String postFix : propertyNameEntry.getValue().getPostFixes()) {
                sql += ", ";
                sql += "a" + count + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.mappedAliasPropertyName(propertyNameEntry.getKey() + postFix, columnNameAliasMapCopy));
            }
            if (propertyCount++ < propertyTypeMap.size()) {
                sql += ", ";
            }
        }
        if (this.getSchemaTable().isEdgeTable()) {
            sql = printEdgeInOutVertexIdOuterFromClauseFor("a" + count, sql, previousSchemaTableTree);
        }
        return sql;
    }

    private static String constructSectionedJoin(SqlgGraph sqlgGraph, SchemaTableTree fromSchemaTableTree, SchemaTableTree toSchemaTableTree, int count) {
        if (toSchemaTableTree.direction == Direction.BOTH) {
            throw new IllegalStateException("Direction may not be BOTH!");
        }
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

        String result;
        if (fromSchemaTableTree.getSchemaTable().getTable().startsWith(EDGE_PREFIX)) {
            if (toSchemaTableTree.isEdgeVertexStep()) {
                if (toSchemaTableTree.direction == Direction.OUT) {
                    result = "a" + (count - 1) + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + Topology.OUT_VERTEX_COLUMN_END);
                    result += " = a" + count + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasId());
                } else {
                    result = "a" + (count - 1) + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + Topology.IN_VERTEX_COLUMN_END);
                    result += " = a" + count + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasId());
                }
            } else {
                if (toSchemaTableTree.direction == Direction.OUT) {
                    result = "a" + (count - 1) + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + Topology.IN_VERTEX_COLUMN_END);
                    result += " = a" + count + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasId());
                } else {
                    result = "a" + (count - 1) + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + Topology.OUT_VERTEX_COLUMN_END);
                    result += " = a" + count + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.lastMappedAliasId());
                }
            }
        } else {
            if (toSchemaTableTree.direction == Direction.OUT) {
                result = "a" + (count - 1) + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + Topology.ID);
                result += " = a" + count + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.mappedAliasVertexForeignKeyColumnEnd(fromSchemaTableTree, toSchemaTableTree.direction, rawFromLabel));
            } else {
                result = "a" + (count - 1) + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + Topology.ID);
                result += " = a" + count + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(toSchemaTableTree.mappedAliasVertexForeignKeyColumnEnd(fromSchemaTableTree, toSchemaTableTree.direction, rawFromLabel));
            }
        }
        return result;
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
            boolean dropStep
    ) {
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
        }

        singlePathSql.append(constructFromClause(sqlgGraph, distinctQueryStack, lastOfPrevious, firstOfNextStack, dropStep));
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
                Traversal.Admin<?, ?> t = (Traversal.Admin<?, ?>) comparator.getValue0();
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
        return this.walkUp((t) -> t.stream().filter(a -> a.endsWith(BaseStrategy.PATH_LABEL_SUFFIX + select)).findAny().isPresent());
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
     * @param distinctQueryStack      //     * @param firstSchemaTableTree    This is the first SchemaTable in the current sql stack. If it is an Edge table then its foreign key
     *                                //     *                                field to the previous table need to be in the select clause in order for the join statement to
     *                                //     *                                reference it.
     *                                //     * @param lastSchemaTableTree
     * @param previousSchemaTableTree The previous schemaTableTree that will be joined to.
     * @param nextSchemaTableTree     represents the table to join to. it is null for the last table as there is nothing to join to.  @return
     * @param dropStep                Indicates that the from clause is generated for a drop step. In this case we only generate the "ID"
     */
    private static String constructFromClause(
            SqlgGraph sqlgGraph,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree previousSchemaTableTree,
            SchemaTableTree nextSchemaTableTree,
            boolean dropStep) {

        SchemaTableTree firstSchemaTableTree = distinctQueryStack.getFirst();
        SchemaTableTree lastSchemaTableTree = distinctQueryStack.getLast();
        SchemaTable firstSchemaTable = firstSchemaTableTree.getSchemaTable();
        SchemaTable lastSchemaTable = lastSchemaTableTree.getSchemaTable();

        if (previousSchemaTableTree != null && previousSchemaTableTree.direction == Direction.BOTH) {
            throw new IllegalStateException("Direction should never be BOTH");
        }
        if (nextSchemaTableTree != null && nextSchemaTableTree.direction == Direction.BOTH) {
            throw new IllegalStateException("Direction should never be BOTH");
        }
        //The join is always between an edge and vertex or vertex and edge table.
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(VERTEX_PREFIX)
                && nextSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 vertex tables!");
        }
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(EDGE_PREFIX)
                && nextSchemaTableTree.getSchemaTable().getTable().startsWith(EDGE_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 edge tables!");
        }

        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(VERTEX_PREFIX)
                && previousSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 vertex tables!");
        }
        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(EDGE_PREFIX)
                && previousSchemaTableTree.getSchemaTable().getTable().startsWith(EDGE_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 edge tables!");
        }

        ColumnList columnList = new ColumnList(sqlgGraph, dropStep);
        boolean printedId = false;

        //join to the previous label/table
        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(EDGE_PREFIX)) {
            if (!previousSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX)) {
                throw new IllegalStateException("Expected table to start with " + VERTEX_PREFIX);
            }
            String previousRawLabel = previousSchemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
            if (firstSchemaTableTree.direction == Direction.OUT) {
                columnList.add(firstSchemaTable,
                        previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                previousRawLabel + Topology.OUT_VERTEX_COLUMN_END,
                        previousSchemaTableTree.stepDepth,
                        firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction));
            } else {
                columnList.add(firstSchemaTable,
                        previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                previousRawLabel + Topology.IN_VERTEX_COLUMN_END,
                        previousSchemaTableTree.stepDepth,
                        firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction));
            }
        } else if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            columnList.add(firstSchemaTable, Topology.ID, firstSchemaTableTree.stepDepth, firstSchemaTableTree.calculatedAliasId());
            printedId = firstSchemaTable == lastSchemaTable;
        }
        //join to the next table/label
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(EDGE_PREFIX)) {
            Preconditions.checkState(nextSchemaTableTree.getSchemaTable().getTable().startsWith(VERTEX_PREFIX), "Expected table to start with %s", VERTEX_PREFIX);

            String nextRawLabel = nextSchemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
            if (nextSchemaTableTree.direction == Direction.OUT) {
                if (nextSchemaTableTree.isEdgeVertexStep()) {
                    columnList.add(lastSchemaTable,
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + Topology.OUT_VERTEX_COLUMN_END,
                            nextSchemaTableTree.stepDepth,
                            lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + Topology.OUT_VERTEX_COLUMN_END);

                    constructAllLabeledFromClause(distinctQueryStack, columnList);
                } else {
                    columnList.add(lastSchemaTable,
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + Topology.IN_VERTEX_COLUMN_END,
                            nextSchemaTableTree.stepDepth,
                            lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + Topology.IN_VERTEX_COLUMN_END);

                    constructAllLabeledFromClause(distinctQueryStack, columnList);
                    constructEmitEdgeIdFromClause(distinctQueryStack, columnList);
                }
            } else {
                if (nextSchemaTableTree.isEdgeVertexStep()) {
                    columnList.add(lastSchemaTable,
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + Topology.IN_VERTEX_COLUMN_END,
                            nextSchemaTableTree.stepDepth,
                            lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + Topology.IN_VERTEX_COLUMN_END);

                    constructAllLabeledFromClause(distinctQueryStack, columnList);
                } else {
                    columnList.add(lastSchemaTable,
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + Topology.OUT_VERTEX_COLUMN_END,
                            nextSchemaTableTree.stepDepth,
                            lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                    nextRawLabel + Topology.OUT_VERTEX_COLUMN_END);

                    constructAllLabeledFromClause(distinctQueryStack, columnList);
                    constructEmitEdgeIdFromClause(distinctQueryStack, columnList);
                }
            }
        } else if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {

            columnList.add(lastSchemaTable,
                    Topology.ID,
                    nextSchemaTableTree.stepDepth,
                    lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + Topology.ID);

            constructAllLabeledFromClause(distinctQueryStack, columnList);

            printedId = firstSchemaTable == lastSchemaTable;
        }

        //The last schemaTableTree in the call stack has no nextSchemaTableTree.
        //This last element's properties need to be returned, including all labeled properties for this path
        if (nextSchemaTableTree == null) {
            if (!printedId) {
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


    private String printLabeledOuterFromClause(String sql, int counter, Map<String, String> columnNameAliasMapCopy) {
        sql += " a" + counter + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.labeledMappedAliasIdForOuterFromClause(columnNameAliasMapCopy));
        Map<String, PropertyType> propertyTypeMap = this.getFilteredAllTables().get(this.getSchemaTable().toString());
        if (!propertyTypeMap.isEmpty()) {
            sql += ", ";
        }
        sql = this.printLabeledOuterFromClauseFor(sql, counter, columnNameAliasMapCopy);
        if (this.getSchemaTable().isEdgeTable()) {
            sql += ", ";
            sql = printLabeledEdgeInOutVertexIdOuterFromClauseFor(sql, counter, columnNameAliasMapCopy);
        }
        return sql;
    }

    private static void constructAllLabeledFromClause(LinkedList<SchemaTableTree> distinctQueryStack, ColumnList cols) {
        List<SchemaTableTree> labeled = distinctQueryStack.stream().filter(d -> !d.getLabels().isEmpty()).collect(Collectors.toList());
        for (SchemaTableTree schemaTableTree : labeled) {
            printLabeledIDFromClauseFor(schemaTableTree, cols);
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
        for (Map.Entry<String, PropertyType> propertyTypeMapEntry : propertyTypeMap.entrySet()) {
            String alias = lastSchemaTableTree.calculateAliasPropertyName(propertyTypeMapEntry.getKey());
            cols.add(lastSchemaTableTree, propertyTypeMapEntry.getKey(), alias);
            for (String postFix : propertyTypeMapEntry.getValue().getPostFixes()) {
                alias = lastSchemaTableTree.calculateAliasPropertyName(propertyTypeMapEntry.getKey() + postFix);
                cols.add(lastSchemaTableTree, propertyTypeMapEntry.getKey() + postFix, alias);
            }
        }
    }

    private String printLabeledOuterFromClauseFor(String sql, int counter, Map<String, String> columnNameAliasMapCopy) {
        Map<String, PropertyType> propertyTypeMap = this.getFilteredAllTables().get(this.getSchemaTable().toString());
        int count = 1;
        for (Map.Entry<String, PropertyType> property : propertyTypeMap.entrySet()) {
            sql += " a" + counter + ".";
            String alias = this.labeledMappedAliasPropertyNameForOuterFromClause(property.getKey(), columnNameAliasMapCopy);
            sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias);
            for (String postFix : property.getValue().getPostFixes()) {
                sql += ", ";
                alias = this.mappedAliasPropertyName(property.getKey() + postFix, columnNameAliasMapCopy);
                sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias);
            }
            if (count++ < propertyTypeMap.size()) {
                sql += ", ";
            }
        }
        return sql;
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

    private String printEdgeInOutVertexIdOuterFromClauseFor(String prepend, String sql, SchemaTableTree previousSchemaTableTree) {
        Preconditions.checkState(this.getSchemaTable().isEdgeTable());
        //Do not print all the edge foreign key ids. Only the edge ids that this outer clause is for.
        Set<String> edgeForeignKeys = this.sqlgGraph.getTopology().getAllEdgeForeignKeys().get(this.getSchemaTable().toString())
                .stream().filter(foreignKeyName ->
                        foreignKeyName.equals(previousSchemaTableTree.getSchemaTable().withOutPrefix().toString() + Topology.IN_VERTEX_COLUMN_END)
                                ||
                                foreignKeyName.equals(previousSchemaTableTree.getSchemaTable().withOutPrefix() + Topology.OUT_VERTEX_COLUMN_END))
                .collect(Collectors.toSet());
        for (String edgeForeignKey : edgeForeignKeys) {
            sql += ", ";
            sql += prepend;
            sql += ".";
            sql += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.mappedAliasPropertyName(edgeForeignKey, this.getColumnNameAliasMap()));
        }
        return sql;
    }

    private static void printEdgeInOutVertexIdFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree firstSchemaTableTree, SchemaTableTree lastSchemaTableTree, ColumnList cols) {
        Preconditions.checkState(lastSchemaTableTree.getSchemaTable().isEdgeTable());

        Set<String> edgeForeignKeys = sqlgGraph.getTopology().getAllEdgeForeignKeys().get(lastSchemaTableTree.getSchemaTable().toString());
        for (String edgeForeignKey : edgeForeignKeys) {
            if (firstSchemaTableTree == null || !firstSchemaTableTree.equals(lastSchemaTableTree) ||
                    firstSchemaTableTree.getDirection() != getDirectionForForeignKey(edgeForeignKey)) {
                String alias = lastSchemaTableTree.calculateAliasPropertyName(edgeForeignKey);
                cols.add(lastSchemaTableTree, edgeForeignKey, alias);
            }
        }

    }

    private static Direction getDirectionForForeignKey(String edgeForeignKey) {
        return edgeForeignKey.endsWith(Topology.IN_VERTEX_COLUMN_END) ? Direction.IN : Direction.OUT;
    }

    private String printLabeledEdgeInOutVertexIdOuterFromClauseFor(String sql, int counter, Map<String, String> columnNameAliasMapCopy) {
        Preconditions.checkState(this.getSchemaTable().isEdgeTable());

        Set<String> edgeForeignKeys = this.sqlgGraph.getTopology().getAllEdgeForeignKeys().get(this.getSchemaTable().toString());
        int propertyCount = 1;
        for (String edgeForeignKey : edgeForeignKeys) {
            sql += " a" + counter + ".";
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.labeledMappedAliasPropertyNameForOuterFromClause(edgeForeignKey, columnNameAliasMapCopy));
            if (propertyCount++ < edgeForeignKeys.size()) {
                sql += ",\n\t";
            }
        }
        return sql;
    }

    private void printLabeledEdgeInOutVertexIdFromClauseFor(ColumnList cols) {
        Preconditions.checkState(this.getSchemaTable().isEdgeTable());

        Set<String> edgeForeignKeys = this.sqlgGraph.getTopology().getAllEdgeForeignKeys().get(this.getSchemaTable().toString());
        for (String edgeForeignKey : edgeForeignKeys) {
            String alias = cols.getAlias(this.getSchemaTable(), edgeForeignKey, this.stepDepth);
            if (alias == null) {
                cols.add(this.getSchemaTable(), edgeForeignKey, this.stepDepth, this.calculateLabeledAliasPropertyName(edgeForeignKey));
            } else {
                this.calculateLabeledAliasPropertyName(edgeForeignKey, alias);
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
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + previousSchemaTableTree.getSchemaTable().getSchema() +
                //This must be a dot as its the foreign key column, i.e. blah__I
                "." + previousRawLabel + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END);
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

    private String labeledMappedAliasPropertyNameForOuterFromClause(String propertyName, Map<String, String> columnNameAliasMapCopy) {
        String reducedLabels = reducedLabels();
        String result = this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        return columnNameAliasMapCopy.get(result);
    }

    private String labeledMappedAliasIdForOuterFromClause(Map<String, String> columnNameAliasMapCopy) {
        String reducedLabels = reducedLabels();
        String result = this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + Topology.ID;
        return columnNameAliasMapCopy.get(result);
    }

    private Optional<String> mappedAliasIdForOuterFromClause(Map<String, String> columnNameAliasMap) {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + Topology.ID;
        return Optional.ofNullable(columnNameAliasMap.get(result));
    }

    private Optional<String> lastMappedAliasIdForOuterFrom(Map<String, String> columnNameAliasMapCopy) {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + Topology.ID;
        return Optional.ofNullable(columnNameAliasMapCopy.get(result));
    }

    private String mappedAliasPropertyName(String propertyName, Map<String, String> columnNameAliasMapCopy) {
        String result = this.stepDepth + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        return columnNameAliasMapCopy.get(result);
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

    private String propertyNameFromLabeledAlias(String alias) {
        //this code is optimized for speed, used to use String.replace but its slow
        String reducedLabels = reducedLabels();
        int lengthToWack = (this.stepDepth + ALIAS_SEPARATOR + reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR).length();
        return alias.substring(lengthToWack);
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
        String joinSql;
        if (leftJoin) {
            joinSql = " LEFT JOIN\n\t";
        } else {
            joinSql = " INNER JOIN\n\t";
        }
        if (fromSchemaTable.getTable().startsWith(VERTEX_PREFIX)) {
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += " ON ";
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

            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema());
            joinSql += ".";
            joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getTable());
            joinSql += " ON ";
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
//                    if (hasContainer.getValue() instanceof Collection) {
//                        Collection<String> labels = (Collection<String>) hasContainer.getValue();
//                        Set<SchemaTable> labelSchemaTables = labels.stream().map(l -> SchemaTable.from(this.sqlgGraph, l)).collect(Collectors.toSet());
//                        BiPredicate<SchemaTable, Collection<SchemaTable>> biPredicate  = (BiPredicate<SchemaTable, Collection<SchemaTable>>) hasContainer.getBiPredicate();
//                        boolean whatever = biPredicate.test(schemaTableTree.getSchemaTable().withOutPrefix(), labelSchemaTables);
//                        if (!whatever) {
//                            return true;
//                        }
//                    } else {
//                        SchemaTable labelSchemaTable = SchemaTable.from(this.sqlgGraph, (String)hasContainer.getValue());
//                        BiPredicate<SchemaTable, SchemaTable> biPredicate  = (BiPredicate<SchemaTable, SchemaTable>) hasContainer.getBiPredicate();
//                        boolean whatever = biPredicate.test(schemaTableTree.getSchemaTable().withOutPrefix(), labelSchemaTable);
//                        if (!whatever) {
//                            return true;
//                        }
//                    }
////                    // we may have been given a type in a schema
////                    SchemaTable predicateSchemaTable = SchemaTable.from(this.sqlgGraph, hasContainer.getValue().toString());
////                    SchemaTable hasContainerLabelSchemaTable = getHasContainerSchemaTable(schemaTableTree, predicateSchemaTable);
////                    if (hasContainer.getBiPredicate().equals(Compare.eq) && !hasContainerLabelSchemaTable.toString().equals(schemaTableTree.getSchemaTable().toString())) {
////                        return true;
////                    }
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
        for (int ix = 1; ix <= resultSet.getMetaData().getColumnCount(); ix++) {
            String columnName = resultSet.getMetaData().getColumnLabel(ix);//entry.getKey();
            Pair<String, PropertyType> p = getColumnNamePropertyName().get(columnName);
            if (p != null) {
                String propertyName = p.getKey();
                PropertyType propertyType = p.getValue();
                if (propertyName.endsWith(Topology.IN_VERTEX_COLUMN_END)) {
                    ((SqlgEdge) sqlgElement).loadInVertex(resultSet, propertyName, ix);
                } else if (propertyName.endsWith(Topology.OUT_VERTEX_COLUMN_END)) {
                    ((SqlgEdge) sqlgElement).loadOutVertex(resultSet, propertyName, ix);
                } else {
                    sqlgElement.loadProperty(resultSet, propertyName, ix, getColumnNameAliasMap(), this.stepDepth, propertyType);
                }
            }
        }
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
        return stepType;
    }

    public List<Pair<Long, Long>> getParentIdsAndIndexes() {
        return parentIdsAndIndexes;
    }

    public void removeDbComparators() {
        this.dbComparators = new ArrayList<>();
        for (SchemaTableTree child : this.children) {
            child.removeDbComparators();
        }
    }

    public boolean isDrop() {
        return drop;
    }

}
