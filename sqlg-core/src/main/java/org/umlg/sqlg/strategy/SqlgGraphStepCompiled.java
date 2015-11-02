package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.process.SqlGraphStepWithPathTraverser;
import org.umlg.sqlg.process.SqlgGraphStepWithPathTraverserGenerator;
import org.umlg.sqlg.sql.parse.AliasMapHolder;
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

    private List<EmitTree<E>> rootEmitTrees = new ArrayList<>();
    private EmitTree<E> currentEmitTree;
    protected Supplier<Iterator<Pair<E, Multimap<String, Emit<E>>>>> iteratorSupplier;
    private List<ReplacedStep<S, E>> replacedSteps = new ArrayList<>();
    private SqlgGraph sqlgGraph;
    private Logger logger = LoggerFactory.getLogger(SqlgGraphStepCompiled.class.getName());
    private Map<SchemaTableTree, List<Pair<LinkedList<SchemaTableTree>, String>>> parsedForStrategySql = new HashMap<>();

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
        SqlGraphStepWithPathTraverser sqlGraphStepWithPathTraverser = (SqlGraphStepWithPathTraverser) this.starts.next();
        Iterator<Emit<E>> toEmit = sqlGraphStepWithPathTraverser.getToEmit().iterator();
        while (toEmit.hasNext()) {
            Emit emit = toEmit.next();
            toEmit.remove();
            if (this.currentEmitTree == null) {
                //get it from the roots
                for (EmitTree<E> rootEmitTree : rootEmitTrees) {
                    if (rootEmitTree.getEmit().getElementPlusEdgeId().equals(emit.getElementPlusEdgeId())) {
                        this.currentEmitTree = rootEmitTree;
                        break;
                    }
                }
            }
            if ((this.rootEmitTrees.isEmpty() || (!this.rootEmitTrees.isEmpty() && this.currentEmitTree == null)) ||
                    (!rootEmitTreeContains(emit) && !this.currentEmitTree.hasChild(emit.getElementPlusEdgeId()))) {

                this.starts.add(sqlGraphStepWithPathTraverser);
                SqlGraphStepWithPathTraverser emitTraverser = new SqlGraphStepWithPathTraverser<>((S) emit.getElementPlusEdgeId().getLeft(), this, 1l);
                emitTraverser.setPath(emit.getPath());
                if (this.currentEmitTree == null) {
                    this.currentEmitTree = new EmitTree<>(emit.getDegree(), emit);
                    this.rootEmitTrees.add(this.currentEmitTree);
                } else {
                    this.currentEmitTree = this.currentEmitTree.addEmit(emit.getDegree(), emit);
                }
                return emitTraverser;
            } else {
                if (!this.currentEmitTree.getEmit().getElementPlusEdgeId().equals(emit.getElementPlusEdgeId())) {
                    this.currentEmitTree = this.currentEmitTree.getChild(emit.getElementPlusEdgeId());
                }
            }
        }
        if (sqlGraphStepWithPathTraverser.get() instanceof SqlgGraphStepWithPathTraverserGenerator.Dummy) {
            //reset the tree to the root
            this.currentEmitTree = null;
            return EmptyTraverser.instance();
        } else {
            //Do not emit the last element if it has already been emitted.
            if (this.currentEmitTree != null) {
                if (this.currentEmitTree.emitEquals((E) sqlGraphStepWithPathTraverser.get())) {
                    if (this.currentEmitTree.getEmit().isUntilFirst()) {
                        //reset the tree to the root
                        this.currentEmitTree = null;
                        return sqlGraphStepWithPathTraverser;
                    } else {
                        //reset the tree to the root
                        this.currentEmitTree = null;
                        return EmptyTraverser.instance();
                    }
                } else {
                    this.currentEmitTree = this.currentEmitTree.addEmit(-1, new Emit((E) sqlGraphStepWithPathTraverser.get(), Optional.empty(), false));
                    this.currentEmitTree = null;
                    return sqlGraphStepWithPathTraverser;
                }
            } else {
                return sqlGraphStepWithPathTraverser;
            }
        }
    }

    private boolean rootEmitTreeContains(Emit emit) {
        for (EmitTree<E> rootEmitTree : rootEmitTrees) {
            if (rootEmitTree.getEmit().getElementPlusEdgeId().equals(emit.getElementPlusEdgeId())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }

    private Iterator<Pair<E, Multimap<String, Emit<E>>>> elements() {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = this.sqlgGraph.getGremlinParser().parse(this.replacedSteps);
        SqlgCompiledResultIterator<Pair<E, Multimap<String, Emit<E>>>> resultIterator = new SqlgCompiledResultIterator<>();
        for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
            AliasMapHolder aliasMapHolder = rootSchemaTableTree.getAliasMapHolder();
            List<LinkedList<SchemaTableTree>> distinctQueries = rootSchemaTableTree.constructDistinctQueries();
            for (LinkedList<SchemaTableTree> distinctQueryStack : distinctQueries) {
                String sql = rootSchemaTableTree.constructSql(distinctQueryStack);
                try {
                    Connection conn = this.sqlgGraph.tx().getConnection();
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql);
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                        SqlgUtil.setParametersOnStatement(this.sqlgGraph, distinctQueryStack, conn, preparedStatement, 1);
                        ResultSet resultSet = preparedStatement.executeQuery();
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        while (resultSet.next()) {
                            AliasMapHolder copyAliasMapHolder = aliasMapHolder.copy();
                            int subQueryDepth = 0;
                            List<LinkedList<SchemaTableTree>> subQueryStacks = SchemaTableTree.splitIntoSubStacks(distinctQueryStack);
                            Multimap<String, Emit<E>> previousLabeledElements = null;

                            //Copy the alias map
                            for (LinkedList<SchemaTableTree> subQueryStack : subQueryStacks) {
                                Multimap<String, Emit<E>> labeledElements = SqlgUtil.loadLabeledElements(
                                        this.sqlgGraph, resultSetMetaData, resultSet, subQueryStack, subQueryDepth, copyAliasMapHolder
                                );
                                if (previousLabeledElements == null) {
                                    previousLabeledElements = labeledElements;
                                } else {
                                    previousLabeledElements.putAll(labeledElements);
                                }
                                //The last subQuery
                                if (subQueryDepth == subQueryStacks.size() - 1) {
                                    Optional<E> e = SqlgUtil.loadLeafElement(
                                            this.sqlgGraph, resultSetMetaData, resultSet, subQueryStack.getLast()
                                    );
                                    //TODO Optional must go all the way up the stack
                                    resultIterator.add(Pair.of((e.isPresent() ? e.get() : null), previousLabeledElements));
                                }
                                subQueryDepth++;
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } finally {
                    rootSchemaTableTree.resetThreadVars();
                }
            }
        }

        stopWatch.stop();
        if (logger.isDebugEnabled())
            logger.debug("SqlgGraphStepCompiled finished, time taken {}", stopWatch.toString());

        return resultIterator;
    }

    void addReplacedStep(ReplacedStep<S, E> replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size() + 1);
        this.replacedSteps.add(replacedStep);
    }

    void parseForStrategy() {
        this.parsedForStrategySql.clear();
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = this.sqlgGraph.getGremlinParser().parseForStrategy(this.replacedSteps);
        for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
            try {
                List<Pair<LinkedList<SchemaTableTree>, String>> sqlStatements = rootSchemaTableTree.constructSql();
                this.parsedForStrategySql.put(rootSchemaTableTree, sqlStatements);
            } finally {
                rootSchemaTableTree.resetThreadVars();
            }
        }
    }

    boolean isForMultipleQueries() {
        return this.parsedForStrategySql.size() > 1 || this.parsedForStrategySql.values().stream().filter(l -> l.size() > 1).count() > 1;
    }

    List<ReplacedStep<S, E>> getReplacedSteps() {
        return replacedSteps;
    }
}
