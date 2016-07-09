package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.LinkedList;
import java.util.Set;

/**
 * Date: 2016/05/04
 * Time: 8:44 PM
 */
public class SqlgSqlExecutor {

    private static Logger logger = LoggerFactory.getLogger(SqlgSqlExecutor.class.getName());

    private SqlgSqlExecutor() {
    }

    public static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeRegularQuery(
            SqlgGraph sqlgGraph,
            SchemaTableTree rootSchemaTableTree,
            RecordId recordId,
            LinkedList<SchemaTableTree> distinctQueryStack) {

        String sql = rootSchemaTableTree.constructSql(distinctQueryStack);
        return executeQuery(sqlgGraph, recordId, sql, distinctQueryStack);
    }

    public static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeOptionalQuery(
            SqlgGraph sqlgGraph, SchemaTableTree rootSchemaTableTree, RecordId recordId,
            Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>> leftJoinQuery) {

        String sql = rootSchemaTableTree.constructSqlForOptional(leftJoinQuery.getLeft(), leftJoinQuery.getRight());
        LinkedList<SchemaTableTree> distinctQueryStack = leftJoinQuery.getLeft();
        return executeQuery(sqlgGraph, recordId, sql, distinctQueryStack);
    }

    public static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeEmitQuery(
            SqlgGraph sqlgGraph, SchemaTableTree rootSchemaTableTree, RecordId recordId,
            LinkedList<SchemaTableTree> leftJoinQuery) {

        String sql = rootSchemaTableTree.constructSqlForEmit(leftJoinQuery);
        return executeQuery(sqlgGraph, recordId, sql, leftJoinQuery);
    }

    private static Triple<ResultSet, ResultSetMetaData, PreparedStatement> executeQuery(SqlgGraph sqlgGraph, RecordId recordId, String sql, LinkedList<SchemaTableTree> distinctQueryStack) {
        try {
            Connection conn = sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql);
            }
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            sqlgGraph.tx().add(preparedStatement);
            int parameterCount = 1;
            if (recordId != null) {
                preparedStatement.setLong(parameterCount++, recordId.getId());
            }
            SqlgUtil.setParametersOnStatement(sqlgGraph, distinctQueryStack, conn, preparedStatement, parameterCount);
//            preparedStatement.setFetchSize(100_000);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            return Triple.of(resultSet, resultSetMetaData, preparedStatement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
