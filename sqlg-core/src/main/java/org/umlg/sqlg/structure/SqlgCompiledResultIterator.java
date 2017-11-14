package org.umlg.sqlg.structure;

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
    private boolean forParent = false;
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

    private List<Emit<SqlgElement>> elements = null;

    /**
     * collect all results if we're not in lazy mode
     */
    private Iterator<List<Emit<SqlgElement>>> allElements = null;
    /**
     * are we reading the query results lazily?
     */
    private boolean lazy = true;

    private boolean first = true;
    private Map<String, Integer> lastElementIdCountMap = new HashMap<>();
    private QUERY queryState = QUERY.REGULAR;

    private enum QUERY {
        REGULAR,
        OPTIONAL,
        EMIT
    }

    public SqlgCompiledResultIterator(SqlgGraph sqlgGraph, Set<SchemaTableTree> rootSchemaTableTrees) {
        this(sqlgGraph, rootSchemaTableTrees, false);
    }

    public SqlgCompiledResultIterator(SqlgGraph sqlgGraph, Set<SchemaTableTree> rootSchemaTableTrees, boolean forParent) {
        this.sqlgGraph = sqlgGraph;
        this.rootSchemaTableTrees = rootSchemaTableTrees;
        this.rootSchemaTableTreeIterator = rootSchemaTableTrees.iterator();
        this.lazy = sqlgGraph.tx().isLazyQueries();
        this.forParent = forParent;
    }

    @Override
    public boolean hasNext() {
        if (this.lazy) {
            return hasNextLazy();
        }
        // eager mode: just read everything about this step and collect it
        if (this.allElements == null) {
            List<List<Emit<SqlgElement>>> allList = new LinkedList<>();
            while (hasNextLazy()) {
                allList.add(this.elements);
                this.elements = null;
            }
            this.allElements = allList.iterator();
        }
        return this.allElements.hasNext();
    }

    /**
     * lazy evaluation of next results
     *
     * @return
     */
    private boolean hasNextLazy() {
        try {
            while (true) {
                switch (this.queryState) {
                    case REGULAR:
                        if (this.elements != null) {
                            return true;
                        } else {
                            if (this.queryResult != null) {
                                iterateRegularQueries();
                                this.first = false;
                            }
                            if (this.elements == null) {
                                closePreparedStatement();
                                //try the next distinctQueryStack
                                if (this.distinctQueriesIterator.hasNext()) {
                                    this.currentDistinctQueryStack = this.distinctQueriesIterator.next();
                                    this.subQueryStacks = SchemaTableTree.splitIntoSubStacks(this.currentDistinctQueryStack);
                                    this.currentRootSchemaTableTree.resetColumnAliasMaps();
                                    if (this.currentDistinctQueryStack.getLast().isDrop()) {
                                        executeDropQuery();
                                        return false;
                                    } else {
                                        executeRegularQuery();
                                    }
                                    this.first = true;
                                } else {
                                    //try the next rootSchemaTableTree
                                    if (this.rootSchemaTableTreeIterator.hasNext()) {
                                        this.currentRootSchemaTableTree = this.rootSchemaTableTreeIterator.next();
                                        this.distinctQueriesIterator = this.currentRootSchemaTableTree.constructDistinctQueries().iterator();
                                    } else {
                                        if (this.currentRootSchemaTableTree != null) {
                                            this.currentRootSchemaTableTree.resetColumnAliasMaps();
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
                        if (this.elements != null) {
                            return true;
                        } else {
                            if (this.queryResult != null) {
                                iterateOptionalQueries();
                                this.first = false;
                            }
                            if (this.elements == null) {
                                closePreparedStatement();
                                //try the next distinctQueryStack
                                if (this.optionalLeftJoinResultsIterator.hasNext()) {
                                    this.optionalCurrentLeftJoinResult = this.optionalLeftJoinResultsIterator.next();
                                    this.subQueryStacks = SchemaTableTree.splitIntoSubStacks(this.optionalCurrentLeftJoinResult.getLeft());
                                    this.currentRootSchemaTableTree.resetColumnAliasMaps();
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
                                            this.currentRootSchemaTableTree.resetColumnAliasMaps();
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
                        if (this.elements != null) {
                            return true;
                        } else {
                            if (this.queryResult != null) {
                                iterateEmitQueries();
                                this.first = false;
                            }
                            if (this.elements == null) {
                                closePreparedStatement();
                                //try the next distinctQueryStack
                                if (this.emitLeftJoinResultsIterator.hasNext()) {
                                    this.emitCurrentLeftJoinResult = this.emitLeftJoinResultsIterator.next();
                                    this.subQueryStacks = SchemaTableTree.splitIntoSubStacks(this.emitCurrentLeftJoinResult);
                                    this.currentRootSchemaTableTree.resetColumnAliasMaps();
                                    executeEmitQuery();
                                    this.first = true;
                                } else {
                                    //try the next rootSchemaTableTree
                                    if (this.rootSchemaTableTreeIterator.hasNext()) {
                                        this.currentRootSchemaTableTree = this.rootSchemaTableTreeIterator.next();
                                        List<LinkedList<SchemaTableTree>> leftJoinResult = new ArrayList<>();
                                        SchemaTableTree.constructDistinctEmitBeforeQueries(this.currentRootSchemaTableTree, leftJoinResult);
                                        this.emitLeftJoinResultsIterator = leftJoinResult.iterator();
                                        if (currentRootSchemaTableTree.isFakeEmit()) {
                                            List<Emit<SqlgElement>> fake = new ArrayList<>();
                                            fake.add(new Emit<>());
                                            this.elements = fake;
                                            this.currentRootSchemaTableTree.setFakeEmit(false);
                                        }
                                    } else {
                                        if (this.currentRootSchemaTableTree != null) {
                                            this.currentRootSchemaTableTree.resetColumnAliasMaps();
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

    @Override
    public E next() {
        if (lazy) {
            return nextLazy();
        }
        return (E) allElements.next();
    }

    /**
     * return the next lazy results
     *
     * @return
     */
    public E nextLazy() {
        //noinspection unchecked
        List<Emit<SqlgElement>> result = this.elements;
        this.elements = null;
        return (E) result;
    }

    private void executeDropQuery() {
        SqlgSqlExecutor.executeDropQuery(this.sqlgGraph, this.currentRootSchemaTableTree, this.currentDistinctQueryStack);
    }

    private void executeRegularQuery() {
        this.queryResult = SqlgSqlExecutor.executeRegularQuery(this.sqlgGraph, this.currentRootSchemaTableTree, this.currentDistinctQueryStack);
    }

    private void executeOptionalQuery() {
        this.queryResult = SqlgSqlExecutor.executeOptionalQuery(this.sqlgGraph, this.currentRootSchemaTableTree, this.optionalCurrentLeftJoinResult);
    }

    private void executeEmitQuery() {
        this.queryResult = SqlgSqlExecutor.executeEmitQuery(this.sqlgGraph, this.currentRootSchemaTableTree, this.emitCurrentLeftJoinResult);
    }

    private void iterateRegularQueries() throws SQLException {
        List<Emit<SqlgElement>> result;
        if (!this.forParent) {
            result = SqlgUtil.loadResultSetIntoResultIterator(
                    this.sqlgGraph,
                    this.queryResult.getMiddle(),
                    this.queryResult.getLeft(),
                    this.currentRootSchemaTableTree,
                    this.subQueryStacks,
                    this.first,
                    this.lastElementIdCountMap
            );
        } else {
            result = SqlgUtil.loadResultSetIntoResultIterator(
                    this.sqlgGraph,
                    this.queryResult.getMiddle(),
                    this.queryResult.getLeft(),
                    this.currentRootSchemaTableTree,
                    this.subQueryStacks,
                    this.first,
                    this.lastElementIdCountMap,
                    true
            );
        }
        if (!result.isEmpty()) {
            this.elements = result;
        }
    }

    private void iterateOptionalQueries() throws SQLException {
        List<Emit<SqlgElement>> result;
        if (!this.forParent) {
            result = SqlgUtil.loadResultSetIntoResultIterator(
                    this.sqlgGraph,
                    this.queryResult.getMiddle(),
                    this.queryResult.getLeft(),
                    this.currentRootSchemaTableTree,
                    this.subQueryStacks,
                    this.first,
                    this.lastElementIdCountMap
            );
        } else {
            result = SqlgUtil.loadResultSetIntoResultIterator(
                    this.sqlgGraph,
                    this.queryResult.getMiddle(),
                    this.queryResult.getLeft(),
                    this.currentRootSchemaTableTree,
                    this.subQueryStacks,
                    this.first,
                    this.lastElementIdCountMap,
                    true
            );
        }
        if (!result.isEmpty()) {
            this.elements = result;
        }
    }

    private void iterateEmitQueries() throws SQLException {
        List<Emit<SqlgElement>> result;
        if (!this.forParent) {
            result = SqlgUtil.loadResultSetIntoResultIterator(
                    this.sqlgGraph,
                    this.queryResult.getMiddle(),
                    this.queryResult.getLeft(),
                    this.currentRootSchemaTableTree,
                    this.subQueryStacks,
                    this.first,
                    this.lastElementIdCountMap
            );
        } else {
            result = SqlgUtil.loadResultSetIntoResultIterator(
                    this.sqlgGraph,
                    this.queryResult.getMiddle(),
                    this.queryResult.getLeft(),
                    this.currentRootSchemaTableTree,
                    this.subQueryStacks,
                    this.first,
                    this.lastElementIdCountMap,
                    true
            );

        }
        if (!result.isEmpty()) {
            this.elements = result;
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

}
