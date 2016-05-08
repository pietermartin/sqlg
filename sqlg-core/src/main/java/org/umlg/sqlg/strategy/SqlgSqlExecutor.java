package org.umlg.sqlg.strategy;

import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.AliasMapHolder;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgCompiledResultIterator;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Date: 2016/05/04
 * Time: 8:44 PM
 */
public class SqlgSqlExecutor {

    private static Logger logger = LoggerFactory.getLogger(SqlgSqlExecutor.class.getName());

    public static <E extends SqlgElement> void executeRegularQueries(
            SqlgGraph sqlgGraph, SchemaTableTree rootSchemaTableTree, RecordId recordId,
            SqlgCompiledResultIterator<Pair<E, Multimap<String, Emit<E>>>> resultIterator) {

        AliasMapHolder aliasMapHolder = rootSchemaTableTree.getAliasMapHolder();
        List<LinkedList<SchemaTableTree>> distinctQueries = rootSchemaTableTree.constructDistinctQueries();
        for (LinkedList<SchemaTableTree> distinctQueryStack : distinctQueries) {
            String sql = rootSchemaTableTree.constructSql(distinctQueryStack);
            try {
                Connection conn = sqlgGraph.tx().getConnection();
                if (logger.isDebugEnabled()) {
                    logger.debug(sql);
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    int parameterCount = 1;
                    if (recordId != null) {
                        preparedStatement.setLong(parameterCount++, recordId.getId());
                    }
                    SqlgUtil.setParametersOnStatement(sqlgGraph, distinctQueryStack, conn, preparedStatement, parameterCount);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                    SqlgUtil.loadResultSetIntoResultIterator(
                            sqlgGraph,
                            resultSetMetaData,
                            resultSet,
                            rootSchemaTableTree,
                            distinctQueryStack,
                            aliasMapHolder,
                            resultIterator);

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                rootSchemaTableTree.resetThreadVars();
            }
        }
    }

    public static <E extends SqlgElement> void executeOptionalQuery(
            SqlgGraph sqlgGraph, SchemaTableTree rootSchemaTableTree, RecordId recordId,
            SqlgCompiledResultIterator<Pair<E, Multimap<String, Emit<E>>>> resultIterator) {

        AliasMapHolder aliasMapHolder = rootSchemaTableTree.getAliasMapHolder();
        //leftJoinResult is a Pair. Left represents the inner join query and right the inner join where there is nothing to join on.
        List<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> leftJoinResult = new ArrayList<>();
        SchemaTableTree.constructDistinctOptionalQueries(rootSchemaTableTree, leftJoinResult);
        for (Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>> leftJoinQuery : leftJoinResult) {
            String sql = rootSchemaTableTree.constructSqlForOptional(leftJoinQuery.getLeft(), leftJoinQuery.getRight());
            try {
                Connection conn = sqlgGraph.tx().getConnection();
                if (logger.isDebugEnabled()) {
                    logger.debug(sql);
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    int parameterCount = 1;
                    if (recordId != null) {
                        preparedStatement.setLong(parameterCount++, recordId.getId());
                    }
                    SqlgUtil.setParametersOnStatement(sqlgGraph, leftJoinQuery.getLeft(), conn, preparedStatement, parameterCount);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                    SqlgUtil.loadResultSetIntoResultIterator(
                            sqlgGraph,
                            resultSetMetaData, resultSet,
                            rootSchemaTableTree,
                            leftJoinQuery.getLeft(),
                            aliasMapHolder,
                            resultIterator);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } finally {
                rootSchemaTableTree.resetThreadVars();
            }
        }
    }

    public static <E extends SqlgElement> void executeEmitQuery(
            SqlgGraph sqlgGraph, SchemaTableTree rootSchemaTableTree, RecordId recordId,
            SqlgCompiledResultIterator<Pair<E, Multimap<String, Emit<E>>>> resultIterator) {

        //leftJoinResult is a Pair. Left represents the inner join query and right the inner join where there is nothing to join on.
        List<LinkedList<SchemaTableTree>> leftJoinResult = new ArrayList<>();
        SchemaTableTree.constructDistinctEmitBeforeQueries(rootSchemaTableTree, leftJoinResult);
        AliasMapHolder aliasMapHolder = rootSchemaTableTree.getAliasMapHolder();
        for (LinkedList<SchemaTableTree> leftJoinQuery : leftJoinResult) {
            String sql = rootSchemaTableTree.constructSqlForEmit(leftJoinQuery);
            try {
                Connection conn = sqlgGraph.tx().getConnection();
                if (logger.isDebugEnabled()) {
                    logger.debug(sql);
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    int parameterCount = 1;
                    if (recordId != null) {
                        preparedStatement.setLong(parameterCount++, recordId.getId());
                    }
                    SqlgUtil.setParametersOnStatement(sqlgGraph, leftJoinQuery, conn, preparedStatement, parameterCount);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                    SqlgUtil.loadResultSetIntoResultIterator(
                            sqlgGraph,
                            resultSetMetaData, resultSet,
                            rootSchemaTableTree,
                            leftJoinQuery,
                            aliasMapHolder,
                            resultIterator);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } finally {
                rootSchemaTableTree.resetThreadVars();
            }
        }

    }
}
