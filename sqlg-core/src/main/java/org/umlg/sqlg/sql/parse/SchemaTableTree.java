package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.umlg.sqlg.strategy.BaseSqlgStrategy;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.SqlgUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.structure.T.label;

/**
 * Date: 2015/01/08
 * Time: 7:06 AM
 */
public class SchemaTableTree {
    public static final String ALIAS_SEPARATOR = "~&~";
    private static final String CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT = "constructSql may only be called on the root object";
    private static final String WITHIN = "within";
    private static final String WITHOUT = "without";
    //stepDepth indicates the depth of the replaced steps. i.e. v1.out().out().out() has stepDepth 0,1,2,3
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
    private List<org.javatuples.Pair<Traversal.Admin, Comparator>> comparators = new ArrayList<>();
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

    //This contains the columnName as key and the generated alias as value
    //Needs to be a multimap as the same column can appear multiple times in different selects in one query
    public static final ThreadLocal<Multimap<String, String>> threadLocalColumnNameAliasMap = new ThreadLocal<Multimap<String, String>>() {
        protected Multimap<String, String> initialValue() {
            return ArrayListMultimap.create();
        }
    };
    //This contains the generated alias as key and the columnName as value
    public static final ThreadLocal<Map<String, String>> threadLocalAliasColumnNameMap = new ThreadLocal<Map<String, String>>() {
        protected Map<String, String> initialValue() {
            return new HashMap<>();
        }
    };

    enum STEP_TYPE {
        GRAPH_STEP,
        VERTEX_STEP,
        EDGE_VERTEX_STEP
    }

    SchemaTableTree(SqlgGraph sqlgGraph, SchemaTable schemaTable, int stepDepth, int replacedStepDepth) {
        this.sqlgGraph = sqlgGraph;
        this.schemaTable = schemaTable;
        this.stepDepth = stepDepth;
        this.hasContainers = new ArrayList<>();
        this.comparators = new ArrayList<>();
        this.labels = Collections.emptySet();
        this.replacedStepDepth = replacedStepDepth;
        this.filteredAllTables = SqlgUtil.filterHasContainers(sqlgGraph.getSchemaManager(), this.hasContainers);
    }

    /**
     * This constructor is called for the root SchemaTableTree(s)
     * <p>
     * This is invoked from {@link ReplacedStep} when creating the root {@link SchemaTableTree}s.
     * The hasContainers at this stage contains the {@link TopologyStrategy} from or without hasContainer.
     * After doing the filtering it must be removed from the hasContainers as it must not partake in sql generation.
     */
    SchemaTableTree(SqlgGraph sqlgGraph, SchemaTable schemaTable, int stepDepth,
                    List<HasContainer> hasContainers,
                    List<org.javatuples.Pair<Traversal.Admin, Comparator>> comparators,
                    STEP_TYPE stepType,
                    boolean emit,
                    boolean untilFirst,
                    boolean optionalLeftJoin,
                    int replacedStepDepth,
                    Set<String> labels
    ) {
        this(sqlgGraph, schemaTable, stepDepth, replacedStepDepth);
        this.hasContainers = hasContainers;
        this.comparators = comparators;
        this.labels = Collections.unmodifiableSet(labels);
        this.stepType = stepType;
        this.emit = emit;
        this.untilFirst = untilFirst;
        this.optionalLeftJoin = optionalLeftJoin;
        this.filteredAllTables = SqlgUtil.filterHasContainers(sqlgGraph.getSchemaManager(), this.hasContainers);
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
                replacedStep.getComparators(),
                replacedStep.getDepth(),
                isEdgeVertexStep,
                replacedStep.isEmit(),
                replacedStep.isUntilFirst(),
                replacedStep.isLeftJoin(),
                labels);
    }

    SchemaTableTree addChild(
            SchemaTable schemaTable,
            Direction direction,
            Class<? extends Element> elementClass,
            ReplacedStep replacedStep,
            Set<String> labels) {
        return addChild(
                schemaTable,
                direction,
                elementClass,
                replacedStep.getHasContainers(),
                replacedStep.getComparators(),
                replacedStep.getDepth(),
                false,
                replacedStep.isEmit(),
                replacedStep.isUntilFirst(),
                replacedStep.isLeftJoin(),
                labels);
    }

    private SchemaTableTree addChild(
            SchemaTable schemaTable,
            Direction direction,
            Class<? extends Element> elementClass,
            List<HasContainer> hasContainers,
            List<org.javatuples.Pair<Traversal.Admin, Comparator>> comparators,
            int stepDepth,
            boolean isEdgeVertexStep,
            boolean emit,
            boolean untilFirst,
            boolean leftJoin,
            Set<String> labels) {

        SchemaTableTree schemaTableTree = new SchemaTableTree(this.sqlgGraph, schemaTable, stepDepth, this.replacedStepDepth);
        if ((elementClass.isAssignableFrom(Edge.class) && schemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) ||
                (elementClass.isAssignableFrom(Vertex.class) && schemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX))) {
            schemaTableTree.hasContainers = new ArrayList<>(hasContainers);
            schemaTableTree.comparators = new ArrayList<>(comparators);
        }
        schemaTableTree.parent = this;
        schemaTableTree.direction = direction;
        this.children.add(schemaTableTree);
        schemaTableTree.stepType = isEdgeVertexStep ? STEP_TYPE.EDGE_VERTEX_STEP : STEP_TYPE.VERTEX_STEP;
        schemaTableTree.labels = Collections.unmodifiableSet(labels);
        schemaTableTree.emit = emit;
        schemaTableTree.untilFirst = untilFirst;
        schemaTableTree.optionalLeftJoin = leftJoin;
        return schemaTableTree;
    }

    private Map<String, Map<String, PropertyType>> getFilteredAllTables() {
        return getRoot().filteredAllTables;
    }

    void initializeAliasColumnNameMaps() {
        this.aliasMapHolder = new AliasMapHolder();
    }

    private Multimap<String, String> getThreadLocalColumnNameAliasMap() {
        return this.getRoot().aliasMapHolder.getColumnNameAliasMap();
    }

    public Map<String, String> getThreadLocalAliasColumnNameMap() {
        return this.getRoot().aliasMapHolder.getAliasColumnNameMap();
    }

    private Map<String, Pair<String, PropertyType>> getColumnNamePropertyName() {
        if (this.columnNamePropertyName == null) {
            this.columnNamePropertyName = new HashMap<>();
            for (Map.Entry<String, String> entry : getRoot().aliasMapHolder.getAliasColumnNameMap().entrySet()) {
                String alias = entry.getKey();
                String columnName = entry.getValue();
                //only load the labelled columns
                if (!columnName.endsWith(SchemaTableTree.ALIAS_SEPARATOR + SchemaManager.ID) &&
                        (columnName.contains(BaseSqlgStrategy.PATH_LABEL_SUFFIX) || columnName.contains(BaseSqlgStrategy.EMIT_LABEL_SUFFIX))) {

                    if (containsLabelledColumn(columnName)) {
                        String propertyName = propertyNameFromLabeledAlias(columnName);
                        PropertyType propertyType = this.sqlgGraph.getSchemaManager().getTableFor(getSchemaTable()).get(propertyName);
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

    public void resetThreadVars() {
        this.aliasMapHolder.clear();
        this.rootAliasCounter = 1;
    }

    private boolean containsLabelledColumn(String columnName) {
        if (columnName.startsWith(this.reducedLabels() + ALIAS_SEPARATOR)) {
            String column = columnName.substring(this.reducedLabels().length() + ALIAS_SEPARATOR.length());
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
            return constructSinglePathSql(this.sqlgGraph, false, distinctQueryStack, null, null);
        }
    }

    public String constructSqlForOptional(LinkedList<SchemaTableTree> innerJoinStack, Set<SchemaTableTree> leftJoinOn) {
        Preconditions.checkState(this.parent == null, CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT);
        if (duplicatesInStack(innerJoinStack)) {
            List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(innerJoinStack);
            return constructDuplicatePathSql(this.sqlgGraph, subQueryStacks, leftJoinOn);
        } else {
            //If there are no duplicates in the path then one select statement will suffice.
            return constructSinglePathSql(this.sqlgGraph, false, innerJoinStack, null, null, leftJoinOn);
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

    /**
     * @return A Triple. SchemaTableTree is the root of the tree that formed the sql statement.
     * It is needed to set the values in the where clause.
     * SchemaTable is the element being returned.
     * String is the sql.
     */
    public List<Pair<LinkedList<SchemaTableTree>, String>> constructSql() {
        Preconditions.checkState(this.parent == null, CONSTRUCT_SQL_MAY_ONLY_BE_CALLED_ON_THE_ROOT_OBJECT);

        List<Pair<LinkedList<SchemaTableTree>, String>> result = new ArrayList<>();
        List<LinkedList<SchemaTableTree>> distinctQueries = constructDistinctQueries();
        for (LinkedList<SchemaTableTree> distinctQueryStack : distinctQueries) {

            //If the same element occurs multiple times in the stack then the sql needs to be different.
            //This is because the same element can not be joined on more than once in sql
            //The way to overcome this is  to break up the path in select sections with no duplicates and then join them together.
            if (duplicatesInStack(distinctQueryStack)) {
                List<LinkedList<SchemaTableTree>> subQueryStacks = splitIntoSubStacks(distinctQueryStack);
                String singlePathSql = constructDuplicatePathSql(this.sqlgGraph, subQueryStacks);
                result.add(Pair.of(distinctQueryStack, singlePathSql));
            } else {
                //If there are no duplicates in the path then one select statement will suffice.
                String singlePathSql = constructSinglePathSql(this.sqlgGraph, false, distinctQueryStack, null, null);
                result.add(Pair.of(distinctQueryStack, singlePathSql));
            }
        }
        return result;
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
        //if its at the full depth it has already been loaded.
        //local step together with emit will create a fake emit. The fake emit will indicate that the incoming traverser must be emitted.
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

    private static String constructDuplicatePathSql(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        return constructDuplicatePathSql(sqlgGraph, subQueryLinkedLists, Collections.emptySet());
    }

    /**
     * Construct a sql statement for one original path to a leaf node.
     * As the path contains the same label more than once it has been split into a List of Stacks.
     */
    private static String constructDuplicatePathSql(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists, Set<SchemaTableTree> leftJoinOn) {
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
                sql = constructSinglePathSql(sqlgGraph, true, subQueryLinkedList, lastOfPrevious, null, leftJoinOn);
            } else {
                sql = constructSinglePathSql(sqlgGraph, true, subQueryLinkedList, lastOfPrevious, firstOfNext);
            }
            singlePathSql += sql;
            if (count == 1) {
                singlePathSql += "\n) a" + count++ + " INNER JOIN (";
            } else {
                //join the last with the first
                singlePathSql += "\n) a" + count + " ON ";
                singlePathSql += constructSectionedJoin(lastOfPrevious, firstSchemaTableTree, count);
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

    private static String constructOuterFromClause(List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        String result = "";
        int countOuter = 1;
        Multimap<String, String> columnNameAliasMapCopy = null;
        LinkedList<SchemaTableTree> previousSubQuery = null;
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {

            int countInner = 1;
            for (SchemaTableTree schemaTableTree : subQueryLinkedList) {

                if (countOuter == 1 && countInner == 1) {
                    //Need a copy here as the entries are remove from the map.
                    //Can not remove it from the original as that map is used later.
                    if (schemaTableTree.getParent() != null) {
                        throw new IllegalStateException("The first SchemaTableTree in the stack must be a root.");
                    }
                    columnNameAliasMapCopy = schemaTableTree.copyColumnNameAliasMap();
                }

                //labelled entries need to be in the outer select
                if (!schemaTableTree.getLabels().isEmpty()) {
                    result = schemaTableTree.printLabeledOuterFromClause(result, countOuter, columnNameAliasMapCopy);
                    result += ", ";
                }
                if (schemaTableTree.getSchemaTable().isEdgeTable() && schemaTableTree.isEmit()) {
                    result += schemaTableTree.printEmitMappedAliasIdForOuterFromClause(countOuter, columnNameAliasMapCopy);
                    result += ", ";
                }
                //last entry, always print this
                if (countOuter == subQueryLinkedLists.size() && countInner == subQueryLinkedList.size()) {
                    @SuppressWarnings("ConstantConditions")
                    SchemaTableTree previousSchemaTableTree = previousSubQuery.getLast();
                    result += schemaTableTree.printOuterFromClause(countOuter, columnNameAliasMapCopy, previousSchemaTableTree);
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

    private String printEmitMappedAliasIdForOuterFromClause(int countOuter, Multimap<String, String> columnNameAliasMap) {
        return " a" + countOuter + ".\"" + this.mappedAliasIdForOuterFromClause(columnNameAliasMap) + "\"";
    }

    private static String constructOuterOrderByClause(SqlgGraph sqlgGraph, List<LinkedList<SchemaTableTree>> subQueryLinkedLists) {
        String result = "";
        int countOuter = 1;
        //construct the order by clause for the comparators
        MutableBoolean mutableOrderBy = new MutableBoolean(false);
        for (LinkedList<SchemaTableTree> subQueryLinkedList : subQueryLinkedLists) {
            int countInner = 1;
            for (SchemaTableTree schemaTableTree : subQueryLinkedList) {
                //last entry, only order on the last entry as duplicate paths are for the same SchemaTable
                if (countOuter == subQueryLinkedLists.size() && countInner == subQueryLinkedList.size()) {
                    result += schemaTableTree.toOrderByClause(sqlgGraph, mutableOrderBy, countOuter);
                }
                countInner++;
            }
            countOuter++;
        }
        return result;
    }

    private String printOuterFromClause(int count, Multimap<String, String> columnNameAliasMapCopy, SchemaTableTree previousSchemaTableTree) {
        String sql = "a" + count + ".\"" + this.lastMappedAliasId(columnNameAliasMapCopy) + "\"";
        Map<String, PropertyType> propertyTypeMap = this.getFilteredAllTables().get(this.toString());
        if (propertyTypeMap.size() > 0) {
            sql += ", ";
        }
        int propertyCount = 1;
        for (Map.Entry<String, PropertyType> propertyNameEntry : propertyTypeMap.entrySet()) {
            sql += "a" + count + ".\"" + this.mappedAliasPropertyName(propertyNameEntry.getKey(), columnNameAliasMapCopy) + "\"";
            for (String postFix : propertyNameEntry.getValue().getPostFixes()) {
                sql += ", ";
                sql += "a" + count + ".\"" + this.mappedAliasPropertyName(propertyNameEntry.getKey() + postFix, columnNameAliasMapCopy) + "\"";
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

    private static String constructSectionedJoin(SchemaTableTree fromSchemaTableTree, SchemaTableTree toSchemaTableTree, int count) {
        if (toSchemaTableTree.direction == Direction.BOTH) {
            throw new IllegalStateException("Direction may not be BOTH!");
        }
        String rawToLabel;
        if (toSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawToLabel = toSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawToLabel = toSchemaTableTree.getSchemaTable().getTable();
        }
        String rawFromLabel;
        if (fromSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawFromLabel = fromSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawFromLabel = fromSchemaTableTree.getSchemaTable().getTable();
        }

        String result;
        if (fromSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (toSchemaTableTree.isEdgeVertexStep()) {
                if (toSchemaTableTree.direction == Direction.OUT) {
                    result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
                    result += " = a" + count + ".\"" + toSchemaTableTree.lastMappedAliasId() + "\"";
                } else {
                    result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
                    result += " = a" + count + ".\"" + toSchemaTableTree.lastMappedAliasId() + "\"";
                }
            } else {
                if (toSchemaTableTree.direction == Direction.OUT) {
                    result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";
                    result += " = a" + count + ".\"" + toSchemaTableTree.lastMappedAliasId() + "\"";
                } else {
                    result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." +
                            toSchemaTableTree.getSchemaTable().getSchema() + "." + rawToLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";
                    result += " = a" + count + ".\"" + toSchemaTableTree.lastMappedAliasId() + "\"";
                }
            }
        } else {
            if (toSchemaTableTree.direction == Direction.OUT) {
                result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                result += " = a" + count + ".\"" + toSchemaTableTree.mappedAliasVertexForeignKeyColumnEnd(fromSchemaTableTree, toSchemaTableTree.direction, rawFromLabel) + "\"";
            } else {
                result = "a" + (count - 1) + ".\"" + fromSchemaTableTree.getSchemaTable().getSchema() + "." + fromSchemaTableTree.getSchemaTable().getTable() + "." + SchemaManager.ID + "\"";
                result += " = a" + count + ".\"" + toSchemaTableTree.mappedAliasVertexForeignKeyColumnEnd(fromSchemaTableTree, toSchemaTableTree.direction, rawFromLabel) + "\"";
            }
        }
        return result;
    }

    private static String constructSinglePathSql(SqlgGraph sqlgGraph, boolean partOfDuplicateQuery, LinkedList<SchemaTableTree> distinctQueryStack, SchemaTableTree lastOfPrevious, SchemaTableTree firstOfNextStack) {
        return constructSinglePathSql(sqlgGraph, partOfDuplicateQuery, distinctQueryStack, lastOfPrevious, firstOfNextStack, Collections.emptySet());
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
    private static String constructSinglePathSql(
            SqlgGraph sqlgGraph,
            boolean partOfDuplicateQuery,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree lastOfPrevious,
            SchemaTableTree firstOfNextStack,
            Set<SchemaTableTree> leftJoinOn) {

        String singlePathSql = "\nSELECT\n\t";
        SchemaTableTree firstSchemaTableTree = distinctQueryStack.getFirst();
        SchemaTable firstSchemaTable = firstSchemaTableTree.getSchemaTable();
        singlePathSql += constructFromClause(sqlgGraph, distinctQueryStack, lastOfPrevious, firstOfNextStack);
        singlePathSql += "\nFROM\n\t";
        singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getSchema());
        singlePathSql += ".";
        singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTableTree.getSchemaTable().getTable());
        SchemaTableTree previous = firstSchemaTableTree;
        boolean skipFirst = true;
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (skipFirst) {
                skipFirst = false;
                continue;
            }
            singlePathSql += constructJoinBetweenSchemaTables(sqlgGraph, previous, schemaTableTree);
            previous = schemaTableTree;
        }

        SchemaTableTree previousLeftJoinSchemaTableTree = null;
        for (SchemaTableTree schemaTableTree : leftJoinOn) {
            if (previousLeftJoinSchemaTableTree == null || !previousLeftJoinSchemaTableTree.getSchemaTable().equals(schemaTableTree.getSchemaTable())) {
                singlePathSql += constructJoinBetweenSchemaTables(sqlgGraph, previous, schemaTableTree, true);
            } else {
                singlePathSql += appendToJoinBetweenSchemaTables(sqlgGraph, previous, schemaTableTree, true);
            }
            previousLeftJoinSchemaTableTree = schemaTableTree;
        }

        //Check if there is a hasContainer with a P.within more than x.
        //If so add in a join to the temporary table that will hold the values of the P.within predicate.
        //These values are inserted/copy command into a temporary table before joining.
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (sqlgGraph.getSqlDialect().supportsBulkWithinOut() && schemaTableTree.hasBulkWithinOrOut(sqlgGraph)) {
                singlePathSql += schemaTableTree.bulkWithJoin(sqlgGraph);
            }
        }

        //lastOfPrevious is null for the first call in the call stack it needs the id parameter in the where clause.
        if (lastOfPrevious == null && distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP) {
            singlePathSql += "\nWHERE\n\t";
            singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema());
            singlePathSql += ".";
            singlePathSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable());
            singlePathSql += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            singlePathSql += " = ? ";
        }


        //check if the 'where' has already been printed
        boolean printedWhere = (lastOfPrevious == null) && (distinctQueryStack.getFirst().stepType != STEP_TYPE.GRAPH_STEP);
        MutableBoolean mutableWhere = new MutableBoolean(printedWhere);
        MutableBoolean mutableOrderBy = new MutableBoolean(false);

        //construct the where clause for the hasContainers
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            singlePathSql += schemaTableTree.toWhereClause(sqlgGraph, mutableWhere);
        }
        //add in the is null where clause for the optional left joins
        for (SchemaTableTree schemaTableTree : leftJoinOn) {
            singlePathSql += schemaTableTree.toOptionalLeftJoinWhereClause(sqlgGraph, mutableWhere);
        }

        //if partOfDuplicateQuery then the order by clause is on the outer select
        if (!partOfDuplicateQuery) {
            //construct the order by clause for the comparators
            for (SchemaTableTree schemaTableTree : distinctQueryStack) {
                singlePathSql += schemaTableTree.toOrderByClause(sqlgGraph, mutableOrderBy, -1);
            }
        }

        return singlePathSql;
    }


    private boolean hasBulkWithinOrOut(SqlgGraph sqlgGraph) {
        return this.hasContainers.stream().filter(h -> SqlgUtil.isBulkWithinAndOut(sqlgGraph, h)).findAny().isPresent();
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

            SecureRandom random = new SecureRandom();
            byte bytes[] = new byte[6];
            random.nextBytes(bytes);
            String tmpTableIdentified = Base64.getEncoder().encodeToString(bytes);
            tmpTableIdentified = SchemaManager.VERTEX_PREFIX + SchemaManager.BULK_TEMP_EDGE + tmpTableIdentified;
            sqlgGraph.getSchemaManager().createTempTable(tmpTableIdentified, columns);

            Map<String, Object> withInOutMap = new HashMap<>();
            if (hasContainer.getBiPredicate() == Contains.within) {
                withInOutMap.put(WITHIN, "unused");
            } else {
                withInOutMap.put(WITHOUT, "unused");
            }
            String copySql = sqlgGraph.getSqlDialect().temporaryTableCopyCommandSqlVertex(sqlgGraph, SchemaTable.of("public", tmpTableIdentified.substring(SchemaManager.VERTEX_PREFIX.length())), withInOutMap);
            OutputStream out = sqlgGraph.getSqlDialect().streamSql(this.sqlgGraph, copySql);

            for (Object withInOutValue : withInOuts) {
                if (withInOutValue instanceof RecordId) {
                    withInOutValue = ((RecordId) withInOutValue).getId();
                }
                withInOutMap = new HashMap<>();
                if (hasContainer.getBiPredicate() == Contains.within) {
                    withInOutMap.put(WITHIN, withInOutValue);
                } else {
                    withInOutMap.put(WITHOUT, withInOutValue);
                }
                sqlgGraph.getSqlDialect().writeStreamingVertex(out, withInOutMap);
            }
            try {
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (hasContainer.getBiPredicate() == Contains.within) {
                sb.append("\nINNER JOIN ");
            } else {
                //left join and in the where clause add a IS NULL, to find the values not in the right hand table
                sb.append("\nLEFT JOIN ");
            }
            sb.append(" \"");
            sb.append(tmpTableIdentified);
            sb.append("\" tmp");
            sb.append(this.rootSchemaTableTree().tmpTableAliasCounter);
            sb.append(" on");
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
        String rawLabel = this.parent.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema()));
        result.append(".");
        result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable()));
        result.append(".");
        result.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                this.parent.getSchemaTable().getSchema() + "." + rawLabel +
                        (this.getDirection() == Direction.IN ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END)));

        result.append(" IS NULL)");
        return result.toString();
    }

    private String toWhereClause(SqlgGraph sqlgGraph, MutableBoolean printedWhere) {
        final StringBuilder result = new StringBuilder();
        if (sqlgGraph.getSqlDialect().supportsBulkWithinOut()) {
            this.hasContainers.stream().filter(h -> !SqlgUtil.isBulkWithin(sqlgGraph, h)).forEach(h -> {
                if (!printedWhere.booleanValue()) {
                    printedWhere.setTrue();
                    result.append("\nWHERE\n\t(");
                } else {
                    result.append(" AND (");
                }
                WhereClause whereClause = WhereClause.from(h.getPredicate());
                result.append(" " + whereClause.toSql(sqlgGraph, this, h) + ")");
            });
        } else {
            for (HasContainer hasContainer : this.getHasContainers()) {
                if (!printedWhere.booleanValue()) {
                    printedWhere.setTrue();
                    result.append("\nWHERE\n\t(");
                } else {
                    result.append(" AND (");
                }
                WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
                result.append(" " + whereClause.toSql(sqlgGraph, this, hasContainer) + ")");
            }
        }
        return result.toString();
    }

    private String toOrderByClause(SqlgGraph sqlgGraph, MutableBoolean printedOrderBy, int counter) {
        String result = "";
        for (org.javatuples.Pair<Traversal.Admin, Comparator> comparator : this.getComparators()) {
            if (!printedOrderBy.booleanValue()) {
                printedOrderBy.setTrue();
                result += "\nORDER BY\n\t";
            } else {
                result += ",\n\t";
            }
            if (comparator.getValue1() instanceof ElementValueComparator) {
                ElementValueComparator elementValueComparator = (ElementValueComparator) comparator.getValue1();
                String prefix = this.getSchemaTable().getSchema();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += this.getSchemaTable().getTable();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += elementValueComparator.getPropertyKey();
                String alias;
                if (counter == -1) {
                    //counter is -1 for single queries, i.e. they are not prefixed with ax
                    alias = sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getThreadLocalColumnNameAliasMap().get(prefix).iterator().next());
                } else {
                    //TODO its a multi map because multiple elements may have the same label
                    alias = "a" + counter + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getThreadLocalColumnNameAliasMap().get(prefix).iterator().next());
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

            } else {
                Preconditions.checkState(comparator.getValue0().getSteps().size() == 1, "toOrderByClause expects a TraversalComparator to have exactly one step!");
                Preconditions.checkState(comparator.getValue0().getSteps().get(0) instanceof SelectOneStep, "toOrderByClause expects a TraversalComparator to have exactly one SelectOneStep!");
                SelectOneStep selectOneStep = (SelectOneStep) comparator.getValue0().getSteps().get(0);
                Preconditions.checkState(selectOneStep.getScopeKeys().size() == 1, "toOrderByClause expects the selectOneStep to have one scopeKey!");
                Preconditions.checkState(selectOneStep.getLocalChildren().size() == 1, "toOrderByClause expects the selectOneStep to have one traversal!");
                Preconditions.checkState(selectOneStep.getLocalChildren().get(0) instanceof ElementValueTraversal, "toOrderByClause expects the selectOneStep's traversal to be a ElementValueTraversal!");

                //need to find the schemaTable that the select is for.
                //this schemaTable is for the leaf node as the order by only occurs last in gremlin (optimized gremlin that is)
                SchemaTableTree selectSchemaTableTree = findSelectSchemaTable((String) selectOneStep.getScopeKeys().iterator().next());
                ElementValueTraversal elementValueTraversal = (ElementValueTraversal) selectOneStep.getLocalChildren().get(0);

                String prefix;
                if (selectSchemaTableTree.children.isEmpty()) {
                    //counter is -1 for single queries, i.e. they are not prefixed with ax
                    prefix = "";
                } else {
                    prefix = selectSchemaTableTree.labels.iterator().next();
                    prefix += SchemaTableTree.ALIAS_SEPARATOR;
                }
                prefix += selectSchemaTableTree.getSchemaTable().getSchema();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += selectSchemaTableTree.getSchemaTable().getTable();
                prefix += SchemaTableTree.ALIAS_SEPARATOR;
                prefix += elementValueTraversal.getPropertyKey();
                String alias;
                if (counter == -1) {
                    //counter is -1 for single queries, i.e. they are not prefixed with ax
                    alias = sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getThreadLocalColumnNameAliasMap().get(prefix).iterator().next());
                } else {
                    //TODO its a multi map because multiple elements may have the same label
                    alias = "a" + selectSchemaTableTree.stepDepth + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getThreadLocalColumnNameAliasMap().get(prefix).iterator().next());
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

    private SchemaTableTree findSelectSchemaTable(String select) {
        return this.walkUp((t) -> t.stream().filter(a -> a.endsWith(BaseSqlgStrategy.PATH_LABEL_SUFFIX + select)).findAny().isPresent());
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

    private static boolean duplicatesInStack(LinkedList<SchemaTableTree> distinctQueryStack) {
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
     */
    private static String constructFromClause(
            SqlgGraph sqlgGraph,
            LinkedList<SchemaTableTree> distinctQueryStack,
            SchemaTableTree previousSchemaTableTree,
            SchemaTableTree nextSchemaTableTree) {

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
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)
                && nextSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 vertex tables!");
        }
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)
                && nextSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 edge tables!");
        }

        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)
                && previousSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 vertex tables!");
        }
        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)
                && previousSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            throw new IllegalStateException("Join can not be between 2 edge tables!");
        }

        String sql = "";
        boolean printedId = false;

        //join to the previous label/table
        if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (!previousSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
                throw new IllegalStateException("Expected table to start with " + SchemaManager.VERTEX_PREFIX);
            }
            String previousRawLabel = previousSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
            if (firstSchemaTableTree.direction == Direction.OUT) {
                sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        previousRawLabel + SchemaManager.OUT_VERTEX_COLUMN_END
                        );
                sql += " AS \"" + firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction) + "\"";
            } else {
                sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                        sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                previousSchemaTableTree.getSchemaTable().getSchema() + "." +
                                        previousRawLabel + SchemaManager.IN_VERTEX_COLUMN_END
                        );
                sql += " AS \"" + firstSchemaTableTree.calculatedAliasVertexForeignKeyColumnEnd(previousSchemaTableTree, firstSchemaTableTree.direction) + "\"";
            }
        } else if (previousSchemaTableTree != null && firstSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getSchema()) + "." +
                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(firstSchemaTable.getTable()) + "." +
                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            sql += " AS \"" + firstSchemaTableTree.calculatedAliasId() + "\"";
            printedId = firstSchemaTable == lastSchemaTable;
        }

        //join to the next table/label
        if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.EDGE_PREFIX)) {
            if (!nextSchemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
                throw new IllegalStateException("Expected table to start with " + SchemaManager.VERTEX_PREFIX);
            }
            String nextRawLabel = nextSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
            if (!sql.isEmpty()) {
                sql += ", ";
            }
            if (nextSchemaTableTree.direction == Direction.OUT) {
                if (nextSchemaTableTree.isEdgeVertexStep()) {
                    sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            nextRawLabel + SchemaManager.OUT_VERTEX_COLUMN_END
                            );
                    sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                            nextRawLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";

                    sql = constructAllLabeledFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);
                } else {
                    sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            nextRawLabel + SchemaManager.IN_VERTEX_COLUMN_END
                            );
                    sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                            nextRawLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";

                    sql = constructAllLabeledFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);
                    sql = constructEmitEdgeIdFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);
                }
            } else {
                if (nextSchemaTableTree.isEdgeVertexStep()) {
                    sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            nextRawLabel + SchemaManager.IN_VERTEX_COLUMN_END
                            );
                    sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                            nextRawLabel + SchemaManager.IN_VERTEX_COLUMN_END + "\"";

                    sql = constructAllLabeledFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);
                } else {
                    sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                            sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                                            nextRawLabel + SchemaManager.OUT_VERTEX_COLUMN_END
                            );
                    sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." +
                            nextSchemaTableTree.getSchemaTable().getSchema() + "." +
                            nextRawLabel + SchemaManager.OUT_VERTEX_COLUMN_END + "\"";

                    sql = constructAllLabeledFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);
                    sql = constructEmitEdgeIdFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);
                }
            }
        } else if (nextSchemaTableTree != null && lastSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            if (!sql.isEmpty()) {
                sql += ", ";
            }
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getSchema()) + "." +
                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTable.getTable()) + "." +
                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
            sql += " AS \"" + lastSchemaTable.getSchema() + "." + lastSchemaTable.getTable() + "." + SchemaManager.ID + "\"";

            sql = constructAllLabeledFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);

            printedId = firstSchemaTable == lastSchemaTable;
        }

        //The last schemaTableTree in the call stack as no nextSchemaTableTree.
        //This last element's properties need to be returned, including all labeled properties for this path
        if (nextSchemaTableTree == null) {
            if (!printedId) {
                sql = printIDFromClauseFor(sqlgGraph, lastSchemaTableTree, sql);
            }
            Map<String, PropertyType> propertyTypeMap = firstSchemaTableTree.getFilteredAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
            if (!propertyTypeMap.isEmpty()) {
                sql += ",\n\t";
            }
            sql = printFromClauseFor(sqlgGraph, lastSchemaTableTree, sql);

            if (lastSchemaTableTree.getSchemaTable().isEdgeTable()) {
                sql += ", ";
                //This is to prevent selecting the same column twice.
                //if first = last then the foreign key has already been printed, however the other direction still needs to be printed
                sql = printEdgeInOutVertexIdFromClauseFor(sqlgGraph, firstSchemaTableTree, lastSchemaTableTree, sql);
            }

            sql = constructAllLabeledFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);
            sql = constructEmitFromClause(sqlgGraph, distinctQueryStack, firstSchemaTableTree, sql);
        }
        return sql;
    }


    private String printLabeledOuterFromClause(String sql, int counter, Multimap<String, String> columnNameAliasMapCopy) {
        sql += " a" + counter + ".\"" + this.labeledMappedAliasId(columnNameAliasMapCopy) + "\"";
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

    private static String constructAllLabeledFromClause(SqlgGraph sqlgGraph, LinkedList<SchemaTableTree> distinctQueryStack, SchemaTableTree firstSchemaTableTree, String sql) {
        Map<String, PropertyType> propertyTypeMap;//all labeled step's properties also need to be returned
        List<SchemaTableTree> labeled = distinctQueryStack.stream().filter(d -> !d.getLabels().isEmpty()).collect(Collectors.toList());
        if (!labeled.isEmpty()) {
            sql += ",\n\t";
        }
        int count = 1;
        for (SchemaTableTree schemaTableTree : labeled) {
            sql = printLabeledIDFromClauseFor(sqlgGraph, schemaTableTree, sql);
            propertyTypeMap = firstSchemaTableTree.getFilteredAllTables().get(schemaTableTree.getSchemaTable().toString());
            if (!propertyTypeMap.isEmpty()) {
                sql += ",\n\t";
            }
            sql = printLabeledFromClauseFor(sqlgGraph, schemaTableTree, sql);
            if (schemaTableTree.getSchemaTable().isEdgeTable()) {
                sql += ",\n\t";
                sql = schemaTableTree.printLabeledEdgeInOutVertexIdFromClauseFor(sql);
            }
            if (count++ < labeled.size()) {
                sql += ",\n\t";
            }
        }
        return sql;
    }

    private static String constructEmitEdgeIdFromClause(SqlgGraph sqlgGraph, LinkedList<SchemaTableTree> distinctQueryStack, SchemaTableTree firstSchemaTableTree, String sql) {
        List<SchemaTableTree> emitted = distinctQueryStack.stream()
                .filter(d -> d.getSchemaTable().isEdgeTable() && d.isEmit())
                .collect(Collectors.toList());
        if (!emitted.isEmpty()) {
            sql += ",\n\t";
        }
        int count = 1;
        for (SchemaTableTree schemaTableTree : emitted) {
            sql = printEdgeId(sqlgGraph, schemaTableTree, sql);
            if (count++ < emitted.size()) {
                sql += ",\n\t";
            }
        }
        return sql;
    }


    /**
     * If emit is true then the edge id also needs to be printed.
     * This is required when there are multiple edges to the same vertex.
     * Only by having access to the edge id can on tell if the vertex needs to be emitted.
     */
    private static String constructEmitFromClause(SqlgGraph sqlgGraph, LinkedList<SchemaTableTree> distinctQueryStack, SchemaTableTree firstSchemaTableTree, String sql) {
        int count = 1;
        for (SchemaTableTree schemaTableTree : distinctQueryStack) {
            if (count > 1) {
                if (!schemaTableTree.getSchemaTable().isEdgeTable() && schemaTableTree.isEmit()) {
                    //if the VertexStep is for an edge table there is no need to print edge ids as its already printed.
                    sql += ",\n\t";
                    sql = printEdgeId(sqlgGraph, schemaTableTree.parent, sql);
                }
            }
            count++;
        }
        return sql;
    }

    private static String printEdgeId(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, String sql) {
        Preconditions.checkArgument(schemaTableTree.getSchemaTable().isEdgeTable());
        sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getSchema()) + "." +
                sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getTable()) + "." +
                sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
        sql += " AS " + sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.calculatedAliasId());
        return sql;
    }

    private static String printIDFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql) {
        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
        if (!sql.isEmpty()) {
            sql += ", ";
        }
        sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
        sql += " AS ";
        sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.calculatedAliasId());
        return sql;
    }

    private static String printFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql) {
        Map<String, PropertyType> propertyTypeMap = lastSchemaTableTree.getFilteredAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
        int propertyCount = 1;
        for (Map.Entry<String, PropertyType> propertyTypeMapEntry : propertyTypeMap.entrySet()) {
            sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyTypeMapEntry.getKey());
            sql += " AS ";
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.calculateAliasPropertyName(propertyTypeMapEntry.getKey()));
            for (String postFix : propertyTypeMapEntry.getValue().getPostFixes()) {
                sql += ", ";
                sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyTypeMapEntry.getKey() + postFix);
                sql += " AS ";
                sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.calculateAliasPropertyName(propertyTypeMapEntry.getKey() + postFix));
            }
            if (propertyCount++ < propertyTypeMap.size()) {
                sql += ",\n\t";
            }
        }
        return sql;
    }


    private String printLabeledOuterFromClauseFor(String sql, int counter, Multimap<String, String> columnNameAliasMapCopy) {
        Map<String, PropertyType> propertyTypeMap = this.getFilteredAllTables().get(this.getSchemaTable().toString());
        int count = 1;
        for (String propertyName : propertyTypeMap.keySet()) {
            sql += " a" + counter + ".";
            sql += "\"";
            sql += this.labeledMappedAliasPropertyName(propertyName, columnNameAliasMapCopy);
            sql += "\"";
            if (count++ < propertyTypeMap.size()) {
                sql += ", ";
            }
        }
        return sql;
    }

    private static String printLabeledIDFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql) {
        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
        sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.ID);
        sql += " AS ";
        sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.calculateLabeledAliasId());
        return sql;
    }

    private static String printLabeledFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree lastSchemaTableTree, String sql) {
        Map<String, PropertyType> propertyTypeMap = lastSchemaTableTree.getFilteredAllTables().get(lastSchemaTableTree.getSchemaTable().toString());
        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());
        int count = 1;
        for (Map.Entry<String, PropertyType> propertyTypeMapEntry : propertyTypeMap.entrySet()) {
            sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyTypeMapEntry.getKey());
            sql += " AS ";
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.calculateLabeledAliasPropertyName(propertyTypeMapEntry.getKey()));

            for (String postFix : propertyTypeMapEntry.getValue().getPostFixes()) {
                sql += ",\n\t";
                sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyTypeMapEntry.getKey() + postFix);
                sql += " AS ";
                sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.calculateAliasPropertyName(propertyTypeMapEntry.getKey() + postFix));
            }

            if (count++ < propertyTypeMap.size()) {
                sql += ",\n\t";
            }
        }
        return sql;
    }

    private String printEdgeInOutVertexIdOuterFromClauseFor(String prepend, String sql, SchemaTableTree previousSchemaTableTree) {
        Preconditions.checkState(this.getSchemaTable().isEdgeTable());
        //Do not print all the edge foreign key ids. Only the edge ids that this outer clause is for.
        Set<String> edgeForeignKeys = this.sqlgGraph.getSchemaManager().getAllEdgeForeignKeys().get(this.getSchemaTable().toString())
                .stream().filter(foreignKeyName ->
                foreignKeyName.equals(previousSchemaTableTree.getSchemaTable().withOutPrefix().toString() + SchemaManager.IN_VERTEX_COLUMN_END)
                ||
                foreignKeyName.equals(previousSchemaTableTree.getSchemaTable().withOutPrefix() + SchemaManager.OUT_VERTEX_COLUMN_END))
                .collect(Collectors.toSet());
        for (String edgeForeignKey : edgeForeignKeys) {
            sql += ", ";
            sql += prepend;
            sql += ".\"";
            sql += this.mappedAliasPropertyName(edgeForeignKey, this.getThreadLocalColumnNameAliasMap());
            sql += "\"";
        }
        return sql;
    }

    private static String printEdgeInOutVertexIdFromClauseFor(SqlgGraph sqlgGraph, SchemaTableTree firstSchemaTableTree, SchemaTableTree lastSchemaTableTree, String sql) {
        Preconditions.checkState(lastSchemaTableTree.getSchemaTable().isEdgeTable());

        String finalFromSchemaTableName = sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.getSchemaTable().getTable());

        Set<String> edgeForeignKeys = sqlgGraph.getSchemaManager().getAllEdgeForeignKeys().get(lastSchemaTableTree.getSchemaTable().toString());
        for (String edgeForeignKey : edgeForeignKeys) {
            if (firstSchemaTableTree == null || !firstSchemaTableTree.equals(lastSchemaTableTree) ||
                    firstSchemaTableTree.getDirection() != getDirectionForForeignKey(edgeForeignKey)) {

                sql += finalFromSchemaTableName + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeForeignKey);
                sql += " AS ";
                sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(lastSchemaTableTree.calculateAliasPropertyName(edgeForeignKey));
                sql += ",\n\t";
            }
        }
        sql = sql.substring(0, sql.length() - 3);
        return sql;
    }

    private static Direction getDirectionForForeignKey(String edgeForeignKey) {
        return edgeForeignKey.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) ? Direction.IN : Direction.OUT;
    }

    private String printLabeledEdgeInOutVertexIdOuterFromClauseFor(String sql, int counter, Multimap<String, String> columnNameAliasMapCopy) {
        Preconditions.checkState(this.getSchemaTable().isEdgeTable());

        Set<String> edgeForeignKeys = this.sqlgGraph.getSchemaManager().getAllEdgeForeignKeys().get(this.getSchemaTable().toString());
        int propertyCount = 1;
        for (String edgeForeignKey : edgeForeignKeys) {
            sql += " a" + counter + ".";
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.labeledMappedAliasPropertyName(edgeForeignKey, columnNameAliasMapCopy));
            if (propertyCount++ < edgeForeignKeys.size()) {
                sql += ",\n\t";
            }
        }
        return sql;
    }

    private String printLabeledEdgeInOutVertexIdFromClauseFor(String sql) {
        Preconditions.checkState(this.getSchemaTable().isEdgeTable());

        String finalFromSchemaTableName = this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getSchema());
        finalFromSchemaTableName += ".";
        finalFromSchemaTableName += this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getSchemaTable().getTable());

        Set<String> edgeForeignKeys = this.sqlgGraph.getSchemaManager().getAllEdgeForeignKeys().get(this.getSchemaTable().toString());
        int propertyCount = 1;
        for (String edgeForeignKey : edgeForeignKeys) {
            sql += finalFromSchemaTableName + "." + this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeForeignKey);
            sql += " AS ";
            sql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.calculateLabeledAliasPropertyName(edgeForeignKey));
            if (propertyCount++ < edgeForeignKeys.size()) {
                sql += ",\n\t";
            }
        }
        return sql;
    }

    private Multimap<String, String> copyColumnNameAliasMap() {
        Multimap<String, String> result = ArrayListMultimap.create();
        Multimap<String, String> copy = this.getThreadLocalColumnNameAliasMap();
        for (String key : copy.keySet()) {
            List<String> values = new ArrayList<>(copy.get(key));
            for (String value : values) {
                result.put(key, value);
            }
        }
        return result;
    }

    private String calculatedAliasId() {
        String result = getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + SchemaManager.ID;
        String alias = rootAliasAndIncrement();
        this.getThreadLocalColumnNameAliasMap().put(result, alias);
        this.getThreadLocalAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String calculateLabeledAliasId() {
        String reducedLabels = reducedLabels();
        String result = reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + SchemaManager.ID;
        String alias = rootAliasAndIncrement();
        this.getThreadLocalColumnNameAliasMap().put(result, alias);
        this.getThreadLocalAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String calculateLabeledAliasPropertyName(String propertyName) {
        String reducedLabels = reducedLabels();
        String result = reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        String alias = rootAliasAndIncrement();
        this.getThreadLocalColumnNameAliasMap().put(result, alias);
        this.getThreadLocalAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String calculateAliasPropertyName(String propertyName) {
        String result = getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        String alias = rootAliasAndIncrement();
        this.getThreadLocalColumnNameAliasMap().put(result, alias);
        this.getThreadLocalAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String calculatedAliasVertexForeignKeyColumnEnd(SchemaTableTree previousSchemaTableTree, Direction direction) {
        String previousRawLabel = previousSchemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        String result = getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + previousSchemaTableTree.getSchemaTable().getSchema() +
                //This must be a dot as its the foreign key column, i.e. balh__I
                "." + previousRawLabel + (direction == Direction.IN ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END);
        String alias = rootAliasAndIncrement();
        this.getThreadLocalColumnNameAliasMap().put(result, alias);
        this.getThreadLocalAliasColumnNameMap().put(alias, result);
        return alias;
    }

    private String mappedAliasVertexForeignKeyColumnEnd(SchemaTableTree previousSchemaTableTree, Direction direction, String rawFromLabel) {
        String result = getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR +
                previousSchemaTableTree.getSchemaTable().getSchema() +
                //This must be a dot as its the foreign key column, i.e. blah__I
                "." + rawFromLabel + (direction == Direction.IN ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END);
        List<String> strings = (List<String>) this.getThreadLocalColumnNameAliasMap().get(result);
        return strings.get(strings.size() - 1);
    }

    private String labeledMappedAliasPropertyName(String propertyName, Multimap<String, String> columnNameAliasMapCopy) {
        String reducedLabels = reducedLabels();
        String result = reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        List<String> strings = (List<String>) columnNameAliasMapCopy.get(result);
        return strings.remove(0);
    }

    private String labeledMappedAliasId(Multimap<String, String> columnNameAliasMapCopy) {
        String reducedLabels = reducedLabels();
        String result = reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + SchemaManager.ID;
        List<String> strings = (List<String>) columnNameAliasMapCopy.get(result);
        return strings.remove(0);
    }

    private String mappedAliasIdForOuterFromClause(Multimap<String, String> columnNameAliasMap) {
        String result = getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + SchemaManager.ID;
        List<String> strings = (List<String>) columnNameAliasMap.get(result);
        return strings.remove(0);
    }

    private String mappedAliasPropertyName(String propertyName, Multimap<String, String> columnNameAliasMapCopy) {
        String result = getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + propertyName;
        List<String> strings = (List<String>) columnNameAliasMapCopy.get(result);
        return strings.get(strings.size() - 1);
    }

    private String lastMappedAliasId() {
        String result = getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + SchemaManager.ID;
        List<String> strings = (List<String>) this.getThreadLocalColumnNameAliasMap().get(result);
        return strings.get(strings.size() - 1);
    }

    private String lastMappedAliasId(Multimap<String, String> columnNameAliasMapCopy) {
        String result = getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + SchemaManager.ID;
        List<String> strings = (List<String>) columnNameAliasMapCopy.get(result);
        return strings.get(strings.size() - 1);
    }

    public String labeledAliasId() {
        if (this.labeledAliasId == null) {
            String reducedLabels = reducedLabels();
            this.labeledAliasId = reducedLabels + ALIAS_SEPARATOR + getSchemaTable().getSchema() + ALIAS_SEPARATOR + getSchemaTable().getTable() + ALIAS_SEPARATOR + SchemaManager.ID;
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
        int lengthToWack = reducedLabels.length() + ALIAS_SEPARATOR.length() + getSchemaTable().getSchema().length() + ALIAS_SEPARATOR.length() + getSchemaTable().getTable().length() + ALIAS_SEPARATOR.length();
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
        if (fromSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawLabel = fromSchemaTable.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawLabel = fromSchemaTable.getTable();
        }
        String rawLabelToTravers;
        if (labelToTravers.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawLabelToTravers = labelToTravers.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawLabelToTravers = labelToTravers.getTable();
        }
        String joinSql;
        if (leftJoin) {
            joinSql = " LEFT JOIN\n\t";
        } else {
            joinSql = " INNER JOIN\n\t";
        }
        if (fromSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
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
                            (labelToTraversTree.getDirection() == Direction.IN ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END)
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
                        rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? SchemaManager.OUT_VERTEX_COLUMN_END : SchemaManager.IN_VERTEX_COLUMN_END));
            } else {
                joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                        rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END));
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
        if (fromSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawLabel = fromSchemaTable.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawLabel = fromSchemaTable.getTable();
        }
        String rawLabelToTravers;
        if (labelToTravers.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
            rawLabelToTravers = labelToTravers.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
        } else {
            rawLabelToTravers = labelToTravers.getTable();
        }
        String joinSql = " OR ";
        if (fromSchemaTable.getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
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
                            (labelToTraversTree.getDirection() == Direction.IN ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END)
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
                        rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? SchemaManager.OUT_VERTEX_COLUMN_END : SchemaManager.IN_VERTEX_COLUMN_END));
            } else {
                joinSql += sqlgGraph.getSqlDialect().maybeWrapInQoutes(labelToTravers.getSchema() + "." +
                        rawLabelToTravers + (labelToTraversTree.getDirection() == Direction.OUT ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END));
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
    void removeAllButDeepestLeafNodes(int depth) {
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

    private void removeObsoleteHasContainers(final SchemaTableTree schemaTableTree) {
        Set<HasContainer> toRemove = new HashSet<>();
        schemaTableTree.hasContainers.forEach(hasContainer -> {
            if (hasContainer.getKey().equals(label.getAccessor()) && hasContainer.getBiPredicate().equals(Compare.eq)) {
                SchemaTable hasContainerLabelSchemaTable;
                if (schemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
                    hasContainerLabelSchemaTable = SchemaTable.from(this.sqlgGraph, SchemaManager.VERTEX_PREFIX + hasContainer.getValue().toString(), this.sqlgGraph.getSqlDialect().getPublicSchema());
                } else {
                    hasContainerLabelSchemaTable = SchemaTable.from(this.sqlgGraph, SchemaManager.EDGE_PREFIX + hasContainer.getValue().toString(), this.sqlgGraph.getSqlDialect().getPublicSchema());
                }
                if (hasContainerLabelSchemaTable.toString().equals(schemaTableTree.getSchemaTable().toString())) {
                    toRemove.add(hasContainer);
                }
            }
        });
        schemaTableTree.hasContainers.removeAll(toRemove);
    }

    private boolean invalidateByHas(SchemaTableTree schemaTableTree) {
        for (HasContainer hasContainer : schemaTableTree.hasContainers) {
            if (!hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_WITHOUT) && !hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_FROM)) {
                if (hasContainer.getKey().equals(label.getAccessor())) {
                    //Check if we are on a vertex or edge
                    SchemaTable hasContainerLabelSchemaTable;
                    if (schemaTableTree.getSchemaTable().getTable().startsWith(SchemaManager.VERTEX_PREFIX)) {
                        hasContainerLabelSchemaTable = SchemaTable.from(this.sqlgGraph, SchemaManager.VERTEX_PREFIX + hasContainer.getValue().toString(), this.sqlgGraph.getSqlDialect().getPublicSchema());
                    } else {
                        hasContainerLabelSchemaTable = SchemaTable.from(this.sqlgGraph, SchemaManager.EDGE_PREFIX + hasContainer.getValue().toString(), this.sqlgGraph.getSqlDialect().getPublicSchema());
                    }
                    if (hasContainer.getBiPredicate().equals(Compare.eq) && !hasContainerLabelSchemaTable.toString().equals(schemaTableTree.getSchemaTable().toString())) {
                        return true;
                    }
                } else if (!hasContainer.getKey().equals(T.id.getAccessor())) {
                    //check if the hasContainer is for a property that exists, if not remove this node from the query tree
                    if (!this.getFilteredAllTables().get(schemaTableTree.getSchemaTable().toString()).containsKey(hasContainer.getKey())) {
                        return true;
                    }
                    //Check if it is a Contains.within with a empty list of values
                    if (hasEmptyWithin(hasContainer)) {
                        return true;
                    }
                } else if (hasEmptyWithin(hasContainer)) {
                    return true;
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
                .append(this.comparators.toString()).append(" ")
                .append(this.direction != null ? this.direction.toString() : "").append(" ")
                .append("isVertexStep = ").append(this.isEdgeVertexStep())
                .append(" isUntilFirst = ").append(this.isUntilFirst())
                .append(" labels = ").append(this.labels);
        for (SchemaTableTree child : children) {
            child.internalToString(sb);
        }
    }

    private SchemaTableTree getParent() {
        return parent;
    }

    public Direction getDirection() {
        return direction;
    }

    public List<HasContainer> getHasContainers() {
        return hasContainers;
    }

    private List<org.javatuples.Pair<Traversal.Admin, Comparator>> getComparators() {
        return this.comparators;
    }

    public int getStepDepth() {
        return stepDepth;
    }

    public int getReplacedStepDepth() {
        return replacedStepDepth;
    }

    public int depth() {
        AtomicInteger depth = new AtomicInteger();
        walk(v -> {
            if (v.stepDepth > depth.get()) {
                depth.set(v.stepDepth);
            }
            return null;
        });
        return depth.get();
    }

    public int numberOfNodes() {
        AtomicInteger count = new AtomicInteger();
        walk(v -> {
            count.getAndIncrement();
            return null;
        });
        return count.get();
    }

    private void walk(Visitor v) {
        v.visit(this);
        this.children.forEach(c -> c.walk(v));
    }

    public SchemaTableTree schemaTableAtDepth(final int depth, final int number) {
        AtomicInteger count = new AtomicInteger();
        //Need to reset the count when the depth changes.
        AtomicInteger depthCache = new AtomicInteger(depth);
        return walkWithExit(
                v -> {
                    if (depthCache.get() != v.stepDepth) {
                        depthCache.set(v.stepDepth);
                        count.set(0);
                    }
                    return (count.getAndIncrement() == number && v.stepDepth == depth);
                }
        );
    }

    private SchemaTableTree walkWithExit(Visitor<Boolean> v) {
        if (!v.visit(this)) {
            if (!this.children.isEmpty()) {
                return this.children.get(0).walkWithExit(v);
            }
        }
        return this;
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
                if (label.contains(BaseSqlgStrategy.PATH_LABEL_SUFFIX)) {
                    this.realLabels.add(label.substring(label.indexOf(BaseSqlgStrategy.PATH_LABEL_SUFFIX) + BaseSqlgStrategy.PATH_LABEL_SUFFIX.length()));
                } else if (label.contains(BaseSqlgStrategy.EMIT_LABEL_SUFFIX)) {
                    this.realLabels.add(label.substring(label.indexOf(BaseSqlgStrategy.EMIT_LABEL_SUFFIX) + BaseSqlgStrategy.EMIT_LABEL_SUFFIX.length()));
                } else {
                    throw new IllegalStateException("label must contain " + BaseSqlgStrategy.PATH_LABEL_SUFFIX + " or " + BaseSqlgStrategy.EMIT_LABEL_SUFFIX);
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
        for (Map.Entry<String, Pair<String, PropertyType>> entry : getColumnNamePropertyName().entrySet()) {
            String columnName = entry.getKey();
            String propertyName = entry.getValue().getKey();
            PropertyType propertyType = entry.getValue().getValue();
            //make sure that if we request an array-backed type, we do it using
            //the getArray() call. Don't bother for byte arrays, because they are
            //handled differently by all supported DBs, so getObject() on them
            //works.
            Object o = (propertyType != null && propertyType.isArray()
                    && propertyType != PropertyType.byte_ARRAY
                    && propertyType != PropertyType.BYTE_ARRAY)
                    ? resultSet.getArray(columnName)
                    : resultSet.getObject(columnName);
            if (!Objects.isNull(o)) {
                if (propertyName.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                    ((SqlgEdge)sqlgElement).loadInVertex(resultSet, propertyName, columnName);
                } else if (propertyName.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                    ((SqlgEdge)sqlgElement).loadOutVertex(resultSet, propertyName, columnName);
                } else {
                    sqlgElement.loadProperty(resultSet, propertyName, o, getThreadLocalColumnNameAliasMap(), propertyType);
                }

            }
        }
    }

    public void clearColumnNamePropertNameMap() {
        if (this.columnNamePropertyName != null) {
            this.columnNamePropertyName.clear();
            this.columnNamePropertyName = null;
        }
    }

    public String idProperty() {
        if (this.idProperty == null) {
            this.idProperty = schemaTable.getSchema() + SchemaTableTree.ALIAS_SEPARATOR + schemaTable.getTable() + SchemaTableTree.ALIAS_SEPARATOR + SchemaManager.ID;
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
}
