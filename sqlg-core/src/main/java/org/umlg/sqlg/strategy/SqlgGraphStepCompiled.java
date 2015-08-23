package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.SqlgCompiledResultIterator;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.function.Supplier;

/**
 * Date: 2015/02/20
 * Time: 9:54 PM
 */
public class SqlgGraphStepCompiled<S, E extends SqlgElement> extends GraphStep {

    protected Supplier<Iterator<Pair<E, Multimap<String, Object>>>> iteratorSupplier;
    private List<ReplacedStep<S, E>> replacedSteps = new ArrayList<>();
    private SqlgGraph sqlgGraph;
    private Logger logger = LoggerFactory.getLogger(SqlgGraphStepCompiled.class.getName());
    private Set<SchemaTableTree> rootSchemaTableTree;

    public SqlgGraphStepCompiled(final SqlgGraph sqlgGraph, final Traversal.Admin traversal, final Class<S> returnClass, final Object... ids) {
        super(traversal, returnClass, ids);
        this.sqlgGraph = sqlgGraph;
        if ((this.ids.length == 0 || !(this.ids[0] instanceof Element))) {
            this.iteratorSupplier = this::elements;
        }
    }

    @Override
    protected Traverser<S> processNextStart() {
        if (this.first) {
            this.start = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
            if (null != this.start) {
                this.starts.add(this.getTraversal().getTraverserGenerator().generateIterator((Iterator<S>) this.start, this, 1l));
            }
            this.first = false;
        }
        Traverser.Admin<S> traverser = this.starts.next();
        return traverser;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }

    private Iterator<Pair<E, Multimap<String, Object>>> elements() {
//        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
//        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Preconditions.checkState(SchemaTableTree.threadLocalAliasColumnNameMap.get().isEmpty(), "Column name and alias thread local map must be empty");
        Preconditions.checkState(SchemaTableTree.threadLocalColumnNameAliasMap.get().isEmpty(), "Column name and alias thread local map must be empty");
//        Set<SchemaTableTree> rootSchemaTableTree = this.sqlgGraph.getGremlinParser().parse(this.replacedSteps);
        SqlgCompiledResultIterator<Pair<E, Multimap<String, Object>>> resultIterator = new SqlgCompiledResultIterator<>();
        for (SchemaTableTree schemaTableTree : this.rootSchemaTableTree) {
            List<Pair<LinkedList<SchemaTableTree>, String>> sqlStatements = schemaTableTree.constructSql();
            try {
                for (Pair<LinkedList<SchemaTableTree>, String> sqlPair : sqlStatements) {
                    Connection conn = this.sqlgGraph.tx().getConnection();
                    if (logger.isDebugEnabled()) {
                        logger.debug(sqlPair.getRight());
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sqlPair.getRight())) {
                        SqlgUtil.setParametersOnStatement(this.sqlgGraph, sqlPair.getLeft(), conn, preparedStatement, 1);
                        ResultSet resultSet = preparedStatement.executeQuery();
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        while (resultSet.next()) {
                            Pair<E, Multimap<String, Object>> result = SqlgUtil.loadElementsLabeledAndEndElements(this.sqlgGraph, resultSetMetaData, resultSet, sqlPair.getLeft());
                            resultIterator.add(result);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                schemaTableTree.resetThreadVars();
            }
        }
        return resultIterator;
    }

    void addReplacedStep(ReplacedStep<S, E> replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size() + 1);
        this.replacedSteps.add(replacedStep);
    }

    void parse() {
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        this.rootSchemaTableTree = this.sqlgGraph.getGremlinParser().parse(this.replacedSteps);
    }

    public Set<SchemaTableTree> getRootSchemaTableTree() {
        return rootSchemaTableTree;
    }
}
