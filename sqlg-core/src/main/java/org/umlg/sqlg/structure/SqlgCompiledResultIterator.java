package org.umlg.sqlg.structure;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.strategy.SqlgSqlExecutor;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Date: 2015/07/01
 * Time: 2:03 PM
 */
public class SqlgCompiledResultIterator<E> implements Iterator<E> {

    private SqlgGraph sqlgGraph;
    private Set<SchemaTableTree> rootSchemaTableTrees;
    private RecordId recordId;
    private Iterator<SchemaTableTree> rootSchemaTableTreeIterator = EmptyIterator.instance();
    private SchemaTableTree currentRootSchemaTableTree;

    private Iterator<LinkedList<SchemaTableTree>> distinctQueriesIterator = EmptyIterator.instance();
    private LinkedList<SchemaTableTree> currentDistinctQueryStack;

    private Iterator<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> optionalLeftJoinResultsIterator = EmptyIterator.instance();
    private Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>> optionalCurrentLeftJoinResult;

    private Iterator<LinkedList<SchemaTableTree>> emitLeftJoinResultsIterator = EmptyIterator.instance();
    private LinkedList<SchemaTableTree> emitCurrentLeftJoinResult;

    private List<LinkedList<SchemaTableTree>> subQueryStacks;

    private Triple<ResultSet, ResultSetMetaData, PreparedStatement> queryResult;

    private List<Pair<SqlgElement, Multimap<String, Emit<SqlgElement>>>> elements = Collections.emptyList();
    private Pair<SqlgElement, Multimap<String, Emit<SqlgElement>>> element;

    private boolean first = true;
    private Map<String, Integer> columnNameCountMap = new HashMap<>();
    private Map<String, Integer> labeledColumnNameCountMap = new HashMap<>();
    private Multimap<String, Integer> lastElementIdCountMap = ArrayListMultimap.create();
    private QUERY queryState = QUERY.REGULAR;

    private enum QUERY {
        REGULAR,
        OPTIONAL,
        EMIT
    }

    SqlgCompiledResultIterator(SqlgGraph sqlgGraph, Set<SchemaTableTree> rootSchemaTableTrees, RecordId recordId) {
        this(sqlgGraph, rootSchemaTableTrees);
        this.recordId = recordId;
    }

    public SqlgCompiledResultIterator(SqlgGraph sqlgGraph, Set<SchemaTableTree> rootSchemaTableTrees) {
        this.sqlgGraph = sqlgGraph;
        this.rootSchemaTableTrees = rootSchemaTableTrees;
        this.rootSchemaTableTreeIterator = rootSchemaTableTrees.iterator();
    }

    @Override
    public boolean hasNext() {
        try {
            while (true) {
                switch (this.queryState) {
                    case REGULAR:
                        if (!this.elements.isEmpty()) {
                            this.element = this.elements.remove(0);
                            return true;
                        } else {
                            if (this.queryResult != null) {
                                iterateRegularQueries();
                                this.first = false;
                            }
                            if (this.elements.isEmpty()) {
                                closePreparedStatement();
                                //try the next distinctQueryStack
                                if (this.distinctQueriesIterator.hasNext()) {
                                    this.currentDistinctQueryStack = this.distinctQueriesIterator.next();
                                    this.subQueryStacks = SchemaTableTree.splitIntoSubStacks(this.currentDistinctQueryStack);
                                    this.currentRootSchemaTableTree.resetThreadVars();
                                    executeRegularQuery();
                                    this.first = true;
                                } else {
                                    //try the next rootSchemaTableTree
                                    if (this.rootSchemaTableTreeIterator.hasNext()) {
                                        this.currentRootSchemaTableTree = this.rootSchemaTableTreeIterator.next();
                                        this.distinctQueriesIterator = this.currentRootSchemaTableTree.constructDistinctQueries().iterator();
                                    } else {
                                        if (this.currentRootSchemaTableTree != null) {
                                            this.currentRootSchemaTableTree.resetThreadVars();
                                        }
                                        this.queryState = QUERY.OPTIONAL;
                                        this.rootSchemaTableTreeIterator = this.rootSchemaTableTrees.iterator();
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    case OPTIONAL:
                        if (!this.elements.isEmpty()) {
                            this.element = this.elements.remove(0);
                            return true;
                        } else {
                            if (this.queryResult != null) {
                                iterateOptionalQueries();
                                this.first = false;
                            }
                            if (this.elements.isEmpty()) {
                                closePreparedStatement();
                                //try the next distinctQueryStack
                                if (this.optionalLeftJoinResultsIterator.hasNext()) {
                                    this.optionalCurrentLeftJoinResult = this.optionalLeftJoinResultsIterator.next();
                                    this.subQueryStacks = SchemaTableTree.splitIntoSubStacks(this.optionalCurrentLeftJoinResult.getLeft());
                                    this.currentRootSchemaTableTree.resetThreadVars();
                                    executeOptionalQuery();
                                    this.first = true;
                                } else {
                                    //try the next rootSchemaTableTree
                                    if (this.rootSchemaTableTreeIterator.hasNext()) {
                                        this.currentRootSchemaTableTree = this.rootSchemaTableTreeIterator.next();
                                        List<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> leftJoinResult = new ArrayList<>();
                                        SchemaTableTree.constructDistinctOptionalQueries(this.currentRootSchemaTableTree, leftJoinResult);
                                        this.optionalLeftJoinResultsIterator = leftJoinResult.iterator();
                                    } else {
                                        if (this.currentRootSchemaTableTree != null) {
                                            this.currentRootSchemaTableTree.resetThreadVars();
                                        }
                                        this.queryState = QUERY.EMIT;
                                        this.rootSchemaTableTreeIterator = this.rootSchemaTableTrees.iterator();
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    case EMIT:
                        if (!this.elements.isEmpty()) {
                            this.element = this.elements.remove(0);
                            return true;
                        } else {
                            if (this.queryResult != null) {
                                iterateEmitQueries();
                                this.first = false;
                            }
                            if (this.elements.isEmpty()) {
                                closePreparedStatement();
                                //try the next distinctQueryStack
                                if (this.emitLeftJoinResultsIterator.hasNext()) {
                                    this.emitCurrentLeftJoinResult = this.emitLeftJoinResultsIterator.next();
                                    this.subQueryStacks = SchemaTableTree.splitIntoSubStacks(this.emitCurrentLeftJoinResult);
                                    this.currentRootSchemaTableTree.resetThreadVars();
                                    executeEmitQuery();
                                    this.first = true;
                                } else {
                                    //try the next rootSchemaTableTree
                                    if (this.rootSchemaTableTreeIterator.hasNext()) {
                                        this.currentRootSchemaTableTree = this.rootSchemaTableTreeIterator.next();
                                        List<LinkedList<SchemaTableTree>> leftJoinResult = new ArrayList<>();
                                        SchemaTableTree.constructDistinctEmitBeforeQueries(this.currentRootSchemaTableTree, leftJoinResult);
                                        this.emitLeftJoinResultsIterator = leftJoinResult.iterator();
                                    } else {
                                        if (this.currentRootSchemaTableTree != null) {
                                            this.currentRootSchemaTableTree.resetThreadVars();
                                        }
                                        return false;
                                    }
                                }
                            }
                        }
                        break;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void closePreparedStatement() {
        if (this.queryResult != null) {
            try {
                this.queryResult.getRight().close();
                this.queryResult = null;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public E next() {
        return (E) this.element;
    }

    private void executeRegularQuery() {
        this.queryResult = SqlgSqlExecutor.executeRegularQuery(this.sqlgGraph, this.currentRootSchemaTableTree, this.recordId, this.currentDistinctQueryStack);
    }

    private void executeOptionalQuery() {
        this.queryResult = SqlgSqlExecutor.executeOptionalQuery(this.sqlgGraph, this.currentRootSchemaTableTree, this.recordId, this.optionalCurrentLeftJoinResult);
    }

    private void executeEmitQuery() {
        this.queryResult = SqlgSqlExecutor.executeEmitQuery(this.sqlgGraph, this.currentRootSchemaTableTree, this.recordId, this.emitCurrentLeftJoinResult);
    }

    private void iterateRegularQueries() throws SQLException {
        this.elements = SqlgUtil.loadResultSetIntoResultIterator(
                this.sqlgGraph,
                this.queryResult.getMiddle(),
                this.queryResult.getLeft(),
                this.currentRootSchemaTableTree,
                this.subQueryStacks,
                this.first,
                this.columnNameCountMap,
                this.labeledColumnNameCountMap,
                this.lastElementIdCountMap);
    }

    private void iterateOptionalQueries() throws SQLException {
        this.elements = SqlgUtil.loadResultSetIntoResultIterator(
                this.sqlgGraph,
                this.queryResult.getMiddle(),
                this.queryResult.getLeft(),
                this.currentRootSchemaTableTree,
                this.subQueryStacks,
                this.first,
                this.columnNameCountMap,
                this.labeledColumnNameCountMap,
                this.lastElementIdCountMap);
    }

    private void iterateEmitQueries() throws SQLException {
        this.elements = SqlgUtil.loadResultSetIntoResultIterator(
                this.sqlgGraph,
                this.queryResult.getMiddle(),
                this.queryResult.getLeft(),
                this.currentRootSchemaTableTree,
                this.subQueryStacks,
                this.first,
                this.columnNameCountMap,
                this.labeledColumnNameCountMap,
                this.lastElementIdCountMap);
    }

}
