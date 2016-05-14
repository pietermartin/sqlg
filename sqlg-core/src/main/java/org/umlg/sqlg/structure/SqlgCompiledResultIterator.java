package org.umlg.sqlg.structure;

import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.umlg.sqlg.sql.parse.AliasMapHolder;
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
    private AliasMapHolder aliasMapHolder;

    private Iterator<LinkedList<SchemaTableTree>> distinctQueriesIterator = EmptyIterator.instance();
    private LinkedList<SchemaTableTree> currentDistinctQueryStack;

    private Iterator<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> optionalLeftJoinResultsIterator = EmptyIterator.instance();
    private Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>> optionalCurrentLeftJoinResult;

    private Iterator<LinkedList<SchemaTableTree>> emitLeftJoinResultsIterator = EmptyIterator.instance();
    private LinkedList<SchemaTableTree> emitCurrentLeftJoinResult;

    private Triple<ResultSet, ResultSetMetaData, PreparedStatement> queryResult;

    private List<Pair<SqlgElement, Multimap<String, Emit<SqlgElement>>>> elements = Collections.emptyList();
    private Pair<SqlgElement, Multimap<String, Emit<SqlgElement>>> element;

    private QUERY queryState = QUERY.REGULAR;

    private enum QUERY {
        REGULAR,
        OPTIONAL,
        EMIT
    }

    public SqlgCompiledResultIterator(SqlgGraph sqlgGraph, Set<SchemaTableTree> rootSchemaTableTrees, RecordId recordId) {
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
        while (true) {
            switch (this.queryState) {
                case REGULAR:
                    if (!this.elements.isEmpty()) {
                        this.element = this.elements.remove(0);
                        return true;
                    } else {
                        if (this.queryResult != null) {
                            iterateRegularQueries();
                        }
                        if (this.elements.isEmpty()) {
                            closePreparedStatement();
                            //try the next distinctQueryStack
                            if (this.distinctQueriesIterator.hasNext()) {
                                this.currentDistinctQueryStack = this.distinctQueriesIterator.next();
                                this.currentRootSchemaTableTree.resetThreadVars();
                                executeRegularQuery();
                            } else {
                                //try the next rootSchemaTableTree
                                if (this.rootSchemaTableTreeIterator.hasNext()) {
                                    this.currentRootSchemaTableTree = this.rootSchemaTableTreeIterator.next();
                                    this.aliasMapHolder = this.currentRootSchemaTableTree.getAliasMapHolder();
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
                        }
                        if (this.elements.isEmpty()) {
                            closePreparedStatement();
                            //try the next distinctQueryStack
                            if (this.optionalLeftJoinResultsIterator.hasNext()) {
                                this.optionalCurrentLeftJoinResult = this.optionalLeftJoinResultsIterator.next();
                                this.currentRootSchemaTableTree.resetThreadVars();
                                executeOptionalQuery();
                            } else {
                                //try the next rootSchemaTableTree
                                if (this.rootSchemaTableTreeIterator.hasNext()) {
                                    this.currentRootSchemaTableTree = this.rootSchemaTableTreeIterator.next();
                                    this.aliasMapHolder = this.currentRootSchemaTableTree.getAliasMapHolder();
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
                        }
                        if (this.elements.isEmpty()) {
                            closePreparedStatement();
                            //try the next distinctQueryStack
                            if (this.emitLeftJoinResultsIterator.hasNext()) {
                                this.emitCurrentLeftJoinResult = this.emitLeftJoinResultsIterator.next();
                                this.currentRootSchemaTableTree.resetThreadVars();
                                executeEmitQuery();
                            } else {
                                //try the next rootSchemaTableTree
                                if (this.rootSchemaTableTreeIterator.hasNext()) {
                                    this.currentRootSchemaTableTree = this.rootSchemaTableTreeIterator.next();
                                    this.aliasMapHolder = this.currentRootSchemaTableTree.getAliasMapHolder();
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
        this.queryResult = SqlgSqlExecutor.executeRegularQuery(sqlgGraph, this.currentRootSchemaTableTree, this.recordId, this.currentDistinctQueryStack);
    }

    private void executeOptionalQuery() {
        this.queryResult = SqlgSqlExecutor.executeOptionalQuery(sqlgGraph, this.currentRootSchemaTableTree, this.recordId, this.optionalCurrentLeftJoinResult);
    }

    private void executeEmitQuery() {
        this.queryResult = SqlgSqlExecutor.executeEmitQuery(sqlgGraph, this.currentRootSchemaTableTree, this.recordId, this.emitCurrentLeftJoinResult);
    }

    private void iterateRegularQueries() {
        this.elements = SqlgUtil.loadResultSetIntoResultIterator(
                this.sqlgGraph,
                this.queryResult.getMiddle(),
                this.queryResult.getLeft(),
                this.currentRootSchemaTableTree,
                this.currentDistinctQueryStack,
                this.aliasMapHolder);

    }

    private void iterateOptionalQueries() {
        this.elements = SqlgUtil.loadResultSetIntoResultIterator(
                sqlgGraph,
                this.queryResult.getMiddle(),
                this.queryResult.getLeft(),
                this.currentRootSchemaTableTree,
                this.optionalCurrentLeftJoinResult.getLeft(),
                this.aliasMapHolder);
    }

    private void iterateEmitQueries() {
        this.elements = SqlgUtil.loadResultSetIntoResultIterator(
                sqlgGraph,
                this.queryResult.getMiddle(),
                this.queryResult.getLeft(),
                this.currentRootSchemaTableTree,
                this.emitCurrentLeftJoinResult,
                this.aliasMapHolder);
    }

}
