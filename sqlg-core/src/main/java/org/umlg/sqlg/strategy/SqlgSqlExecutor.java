package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
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

    private static final Logger logger = LoggerFactory.getLogger(SqlgSqlExecutor.class);

    private SqlgSqlExecutor() {
    }

    public enum DROP_QUERY {
        ALTER,
        EDGE,
        NORMAL,
        TRUNCATE
    }

    public static void executeDropQuery(
            SqlgGraph sqlgGraph,
            SchemaTableTree rootSchemaTableTree,
            LinkedList<SchemaTableTree> distinctQueryStack) {

        sqlgGraph.getTopology().threadWriteLock();
        List<Triple<DROP_QUERY, String, Boolean>> sqls = rootSchemaTableTree.constructDropSql(distinctQueryStack);
        for (Triple<DROP_QUERY, String, Boolean> sqlPair : sqls) {
            DROP_QUERY dropQuery = sqlPair.getLeft();
            String sql = sqlPair.getMiddle();
            Boolean addAdditionalPartitionHasContainer = sqlPair.getRight();
            switch (dropQuery) {
                case ALTER:
                case TRUNCATE:
                    executeDropQuery(sqlgGraph, sql, new LinkedList<>(), false);
                    break;
                case EDGE:
                    LinkedList<SchemaTableTree> tmp = new LinkedList<>(distinctQueryStack);
                    tmp.removeLast();
                    executeDropQuery(sqlgGraph, sql, tmp, false);
                    break;
                case NORMAL:
                    executeDropQuery(sqlgGraph, sql, distinctQueryStack, addAdditionalPartitionHasContainer);
                    break;
                default:
                    throw new IllegalStateException("Unknown DROP_QUERY " + dropQuery);
            }
        }
    }

    public static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeRegularQuery(
            SqlgGraph sqlgGraph,
            SchemaTableTree rootSchemaTableTree,
            LinkedList<SchemaTableTree> distinctQueryStack) {

        String sql = rootSchemaTableTree.constructSql(distinctQueryStack);
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
        try {
            if (distinctQueryStack.peekFirst().getStepType() != SchemaTableTree.STEP_TYPE.GRAPH_STEP) {
                Preconditions.checkState(!distinctQueryStack.peekFirst().getParentIdsAndIndexes().isEmpty());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql);
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
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            sqlgGraph.tx().add(preparedStatement);
            int parameterCount = 1;
            SqlgUtil.setParametersOnStatement(sqlgGraph, distinctQueryStack, preparedStatement, parameterCount);
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
            if (logger.isDebugEnabled()) {
                logger.debug(sql);
            }
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            sqlgGraph.tx().add(preparedStatement);
            int parameterCount = 1;
            SqlgUtil.setParametersOnStatement(sqlgGraph, distinctQueryStack, preparedStatement, parameterCount, includeAdditionalPartitionHasContainer);
            int deleteCount;
            if (distinctQueryStack.isEmpty()) {
                deleteCount = preparedStatement.executeUpdate();
            } else {
                deleteCount = preparedStatement.executeUpdate();
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Deleted {} rows", deleteCount);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void executeDropEdges(SqlgGraph sqlgGraph, EdgeLabel edgeLabel, String sql, List<EventCallback<Event>> mutatingCallbacks) {
        try {
            Connection conn = sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql);
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
            if (logger.isDebugEnabled()) {
                logger.debug(sql);
            }
            try (Statement statement = conn.createStatement()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
