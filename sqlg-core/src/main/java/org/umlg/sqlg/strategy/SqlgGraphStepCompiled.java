package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.process.SqlGraphStepWithPathTraverser;
import org.umlg.sqlg.sql.parse.AliasMapHolder;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.Dummy;
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
public class SqlgGraphStepCompiled<S extends SqlgElement, E extends SqlgElement> extends GraphStep implements SqlgStep {

    private List<EmitTree<E>> rootEmitTrees = new ArrayList<>();
    private EmitTree<E> currentEmitTree;
    private Supplier<Iterator<Pair<E, Multimap<String, Emit<E>>>>> iteratorSupplier;
    private List<ReplacedStep<S, E>> replacedSteps = new ArrayList<>();
    private SqlgGraph sqlgGraph;
    private Logger logger = LoggerFactory.getLogger(SqlgGraphStepCompiled.class.getName());
    private Map<SchemaTableTree, List<Pair<LinkedList<SchemaTableTree>, String>>> parsedForStrategySql = new HashMap<>();
    private Emit lastEmit = null;
    private boolean first = true;

    public SqlgGraphStepCompiled(final SqlgGraph sqlgGraph, final Traversal.Admin traversal, final Class<S> returnClass, final boolean isStart, final Object... ids) {
        super(traversal, returnClass, isStart, ids);
        this.sqlgGraph = sqlgGraph;
        this.iteratorSupplier = this::elements;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {

        if (this.first) {
            this.starts.add(this.getTraversal().getTraverserGenerator().generateIterator(this.iteratorSupplier.get(), this, 1l));
            this.first = false;
        }

        SqlGraphStepWithPathTraverser sqlGraphStepWithPathTraverser = (SqlGraphStepWithPathTraverser) this.starts.next();
        Iterator<Emit<E>> toEmit = sqlGraphStepWithPathTraverser.getToEmit().iterator();
        while (toEmit.hasNext()) {
            Emit emit = toEmit.next();
            toEmit.remove();
            lastEmit = emit;
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
                    (!rootEmitTreeContains(this.rootEmitTrees, emit) && !this.currentEmitTree.hasChild(emit.getElementPlusEdgeId()))) {

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
        if (sqlGraphStepWithPathTraverser.get() instanceof Dummy) {
            //reset the tree to the root
            this.currentEmitTree = null;
            return EmptyTraverser.instance();
        } else {
            if (this.currentEmitTree != null) {
                this.currentEmitTree = null;
            }
            //if emit and times are both at the end or start then only emit or return but not both.
            if (lastEmit != null && lastEmit.emitAndUntilBothAtStart()) {
                return sqlGraphStepWithPathTraverser;
            } else  if (lastEmit != null && this.lastEmit.emitAndUntilBothAtEnd()) {
                return EmptyTraverser.instance();
            } else {
                return sqlGraphStepWithPathTraverser;
            }
        }
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

                        SqlgUtil.loadResultSetIntoResultIterator(
                                this.sqlgGraph,
                                resultSetMetaData, resultSet,
                                rootSchemaTableTree, distinctQueryStack,
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

        stopWatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("SqlgGraphStepCompiled finished, time taken {}", stopWatch.toString());
        }

        return resultIterator;
    }

    @Override
    public void addReplacedStep(ReplacedStep replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size());
        this.replacedSteps.add(replacedStep);
    }

    @Override
    public void parseForStrategy() {
        this.parsedForStrategySql.clear();
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = this.sqlgGraph.getGremlinParser().parseForStrategy(this.replacedSteps);
        for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
            try {
                //TODO this really sucks, constructsql should not query, but alas it does for P.within and temp table jol
                if (this.sqlgGraph.tx().isOpen() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
                    throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
                }
                List<Pair<LinkedList<SchemaTableTree>, String>> sqlStatements = rootSchemaTableTree.constructSql();
                this.parsedForStrategySql.put(rootSchemaTableTree, sqlStatements);
            } finally {
                rootSchemaTableTree.resetThreadVars();
            }
        }
    }

    public boolean isForMultipleQueries() {
        return this.parsedForStrategySql.size() > 1 || this.parsedForStrategySql.values().stream().filter(l -> l.size() > 1).count() > 0;
    }

    public List<ReplacedStep<S, E>> getReplacedSteps() {
        return replacedSteps;
    }

}
