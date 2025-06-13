package org.umlg.sqlg.strategy;

import org.umlg.sqlg.util.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.SchemaTableTreeCache;
import org.umlg.sqlg.structure.SqlgEdge;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Date: 2016/05/04
 * Time: 8:44 PM
 */
public class SqlgSqlExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlgSqlExecutor.class);

    public record DropQuery(SqlgSqlExecutor.DROP_QUERY dropQuery, String leafSql, String sql,
                            Boolean addAdditionalPartitionHasContainer) {
    }

    private SqlgSqlExecutor() {
    }

    public enum DROP_QUERY {
        ALTER,
        EDGE,
        NORMAL,
        TRUNCATE,
        MULTIPLICITY
    }

    public static void executeDropQuery(
            SqlgGraph sqlgGraph,
            SchemaTableTree rootSchemaTableTree,
            LinkedList<SchemaTableTree> distinctQueryStack) {

        sqlgGraph.getTopology().threadWriteLock();
        List<DropQuery> sqls = rootSchemaTableTree.constructDropSql(distinctQueryStack);
        for (DropQuery sqlPair : sqls) {
            DROP_QUERY dropQuery = sqlPair.dropQuery();
            String sql = sqlPair.sql();
            Boolean addAdditionalPartitionHasContainer = sqlPair.addAdditionalPartitionHasContainer();
            switch (dropQuery) {
                case ALTER, TRUNCATE -> executeDropQuery(sqlgGraph, sql, new LinkedList<>(), false);
                case EDGE -> {
                    LinkedList<SchemaTableTree> tmp = new LinkedList<>(distinctQueryStack);
                    tmp.removeLast();
                    executeDropQuery(sqlgGraph, sql, tmp, false);
                }
                case NORMAL -> executeDropQuery(sqlgGraph, sql, distinctQueryStack, addAdditionalPartitionHasContainer);
                default -> throw new IllegalStateException("Unknown DROP_QUERY " + dropQuery);
            }
        }
    }

    public static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeRegularQuery(
            SqlgGraph sqlgGraph,
            SchemaTableTree rootSchemaTableTree,
            LinkedList<SchemaTableTree> distinctQueryStack) {

        String sql;
        if (sqlgGraph.configuration().getBoolean("gremlin.cache.enabled", false)) {
            SchemaTableTreeCache CACHE = sqlgGraph.getSchemaTableTreeCache();
            Pair<SchemaTableTree, LinkedList<SchemaTableTree>> p = Pair.of(rootSchemaTableTree, distinctQueryStack);
            sql = CACHE.sql(p);
        } else {
            sql = rootSchemaTableTree.constructSql(distinctQueryStack);
        }
        return executeQuery(sqlgGraph, sql, distinctQueryStack);
    }

    public static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeOptionalQuery(
            SqlgGraph sqlgGraph, SchemaTableTree rootSchemaTableTree,
            Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>> leftJoinQuery) {

        String sql = rootSchemaTableTree.constructSqlForOptional(leftJoinQuery.getLeft(), leftJoinQuery.getRight());
        LinkedList<SchemaTableTree> distinctQueryStack = leftJoinQuery.getLeft();
        return executeQuery(sqlgGraph, sql, distinctQueryStack);
    }

    public static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeEmitQuery(
            SqlgGraph sqlgGraph, SchemaTableTree rootSchemaTableTree,
            LinkedList<SchemaTableTree> leftJoinQuery) {

        String sql = rootSchemaTableTree.constructSql(leftJoinQuery);
        return executeQuery(sqlgGraph, sql, leftJoinQuery);
    }

    private static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeQuery(SqlgGraph sqlgGraph, String sql, LinkedList<SchemaTableTree> distinctQueryStack) {
        if (sqlgGraph.tx().isInBatchMode()) {
            sqlgGraph.tx().flush();
        }
        //noinspection CommentedOutCode
        try {
            if (!distinctQueryStack.isEmpty() && distinctQueryStack.peekFirst().getStepType() != SchemaTableTree.STEP_TYPE.GRAPH_STEP) {
                Preconditions.checkState(!distinctQueryStack.isEmpty() && !distinctQueryStack.peekFirst().getParentIdsAndIndexes().isEmpty());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql);
            }
            // explain plan can be useful for performance issues
            // uncomment if needed, don't think we need this in production
//            if (logger.isTraceEnabled()){
//            	String expl="EXPLAIN "+sql;
//            	try {
//	            	try (PreparedStatement stmt=conn.prepareStatement(expl)){
//	                    int parameterCount = 1;
//	                    if (recordId != null) {
//	                        stmt.setLong(parameterCount++, recordId.getId());
//	                    }
//	                    SqlgUtil.setParametersOnStatement(sqlgGraph, distinctQueryStack, conn, stmt, parameterCount);
//	            		try(ResultSet rs=stmt.executeQuery()){
//	            			while(rs.next()){
//	            				System.out.println(rs.getString(1));
//	            			}
//	            		}
//	            		
//	            		
//	            	}
//            	} catch (SQLException sqle){
//            		logger.warn(expl);
//            		logger.warn(sqle.getMessage());
//            	}
//            }
            PreparedStatement preparedStatement = conn.prepareStatement(
                    sql,
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY
            );
            sqlgGraph.tx().add(preparedStatement);
            SqlgUtil.setParametersOnStatement(sqlgGraph, distinctQueryStack, preparedStatement, false);
            // https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
            // this is critical to use a cursor, otherwise we load everything into memory
            if (sqlgGraph.tx().getFetchSize() != null) {
                preparedStatement.setFetchSize(sqlgGraph.tx().getFetchSize());
            }
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            return Triple.of(resultSet, resultSetMetaData, preparedStatement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeDropQuery(SqlgGraph sqlgGraph, String sql, LinkedList<SchemaTableTree> distinctQueryStack, boolean includeAdditionalPartitionHasContainer) {
        if (sqlgGraph.tx().isInBatchMode()) {
            sqlgGraph.tx().flush();
        }
        try {
            if (!distinctQueryStack.isEmpty() && distinctQueryStack.peekFirst().getStepType() != SchemaTableTree.STEP_TYPE.GRAPH_STEP) {
                Preconditions.checkState(distinctQueryStack.peekFirst() != null);
                Preconditions.checkState(!distinctQueryStack.peekFirst().getParentIdsAndIndexes().isEmpty());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql);
            }
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            sqlgGraph.tx().add(preparedStatement);
            SqlgUtil.setParametersOnStatement(sqlgGraph, distinctQueryStack, preparedStatement, includeAdditionalPartitionHasContainer);
            int deleteCount;
            if (distinctQueryStack.isEmpty()) {
                deleteCount = preparedStatement.executeUpdate();
            } else {
                deleteCount = preparedStatement.executeUpdate();
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Deleted {} rows", deleteCount);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void executeDropEdges(SqlgGraph sqlgGraph, EdgeLabel edgeLabel, String sql, List<EventCallback<Event>> mutatingCallbacks) {
        try {
            Connection conn = sqlgGraph.tx().getConnection();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql);
            }
            try (Statement statement = conn.createStatement()) {
                if (mutatingCallbacks.isEmpty()) {
                    statement.execute(sql);
                } else {
                    ResultSet resultSet = statement.executeQuery(sql);
                    while (resultSet.next()) {
                        Long id = resultSet.getLong(1);
                        final Event removeEvent;
                        removeEvent = new Event.EdgeRemovedEvent(SqlgEdge.of(sqlgGraph, id, edgeLabel.getSchema().getName(), edgeLabel.getName()));
                        for (EventCallback<Event> eventCallback : mutatingCallbacks) {
                            eventCallback.accept(removeEvent);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static void executeDrop(SqlgGraph sqlgGraph, String sql) {
        try {
            Connection conn = sqlgGraph.tx().getConnection();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql);
            }
            try (Statement statement = conn.createStatement()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
