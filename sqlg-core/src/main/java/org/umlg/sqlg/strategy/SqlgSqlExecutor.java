package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.SqlgGraph;
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

    private static Logger logger = LoggerFactory.getLogger(SqlgSqlExecutor.class.getName());

    private SqlgSqlExecutor() {
    }

    public static boolean executeDropQuery(
            SqlgGraph sqlgGraph,
            SchemaTableTree rootSchemaTableTree,
            LinkedList<SchemaTableTree> distinctQueryStack) {

        List<String> sqls = rootSchemaTableTree.constructDropSql(distinctQueryStack);
        int count = 1;
        for (String sql : sqls) {
            if (count++ == sqls.size() - 1) {
                distinctQueryStack.removeLast();
                executeDropQuery(sqlgGraph, sql, distinctQueryStack);
            } else {
                if (sql.startsWith("ALTER")) {
                    executeDropQuery(sqlgGraph, sql, new LinkedList<>());
                } else {
                    executeDropQuery(sqlgGraph, sql, distinctQueryStack);
                }
            }
        }
        return true;
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

        String sql = rootSchemaTableTree.constructSqlForEmit(leftJoinQuery);
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
//            preparedStatement.setFetchSize(100_000);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            return Triple.of(resultSet, resultSetMetaData, preparedStatement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeDropQuery(SqlgGraph sqlgGraph, String sql, LinkedList<SchemaTableTree> distinctQueryStack) {
        if (sqlgGraph.tx().isInBatchMode()) {
            sqlgGraph.tx().flush();
        }
        try {
            if (!distinctQueryStack.isEmpty() && distinctQueryStack.peekFirst().getStepType() != SchemaTableTree.STEP_TYPE.GRAPH_STEP) {
                Preconditions.checkState(!distinctQueryStack.peekFirst().getParentIdsAndIndexes().isEmpty());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql);
            }
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            sqlgGraph.tx().add(preparedStatement);
            int parameterCount = 1;
            SqlgUtil.setParametersOnStatement(sqlgGraph, distinctQueryStack, preparedStatement, parameterCount);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
