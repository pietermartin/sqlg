package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.process.EmitOrderAndRangeHelper;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.SqlgCompiledResultIterator;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;

/**
 * Date: 2015/02/20
 * Time: 9:54 PM
 */
public class SqlgGraphStepCompiled<S, E extends SqlgElement> extends GraphStep implements SqlgStep, TraversalParent {

    private Logger logger = LoggerFactory.getLogger(SqlgGraphStepCompiled.class.getName());

    private List<ReplacedStep<?, ?>> replacedSteps = new ArrayList<>();
    private ReplacedStepTree replacedStepTree;

    private SqlgGraph sqlgGraph;
    private Map<SchemaTableTree, List<Pair<LinkedList<SchemaTableTree>, String>>> parsedForStrategySql = new HashMap<>();

    private Emit<E> toEmit = null;
    private Iterator<List<Emit<E>>> elementIter;
    private List<Emit<E>> eagerLoadedResults = new ArrayList<>();
    private Iterator<Emit<E>> eagerLoadedResultsIter;
    private Traverser.Admin<S> previousHead;

    SqlgGraphStepCompiled(final SqlgGraph sqlgGraph, final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final Object... ids) {
        super(traversal, returnClass, isStart, ids);
        this.sqlgGraph = sqlgGraph;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (this.eagerLoadedResultsIter != null && this.eagerLoadedResultsIter.hasNext()) {
                Traverser.Admin<E> traverser = null;
                Emit<E> emit = this.eagerLoadedResultsIter.next();
                boolean first = true;
                Iterator<Set<String>> labelIter = emit.getPath().labels().iterator();
                for (Object o : emit.getPath().objects()) {
                    E e = (E) o;
                    Set<String> labels = labelIter.next();
                    this.labels = labels;
                    if (!isStart && previousHead != null && traverser == null) {
                        traverser = previousHead.split(e, this);
                    } else if (first) {
                        first = false;
                        traverser = B_LP_O_P_S_SE_SL_TraverserGenerator.instance().generate((S) e, this, 1L);
                    } else {
                        traverser = ((B_LP_O_P_S_SE_SL_Traverser) traverser).split(e, this);
                    }
                }
                return traverser;
            } else {
                if (!this.isStart) {
                    this.previousHead = this.starts.next();
                } else {
                    if (this.done) {
                        throw FastNoSuchElementException.instance();
                    }
                    this.done = true;
                }
                this.elementIter = elements();
                eagerLoad();
                EmitOrderAndRangeHelper emitOrderAndRangeHelper = new EmitOrderAndRangeHelper<>(this.eagerLoadedResults, this.replacedSteps);
                emitOrderAndRangeHelper.sortAndApplyRange();
                this.eagerLoadedResultsIter = this.eagerLoadedResults.iterator();
            }
        }
    }

    private boolean flattenRawIterator() {
        if (this.elementIter.hasNext()) {
            List<Emit<E>> emits = this.elementIter.next();
            List<SqlgComparatorHolder> emitComparators = new ArrayList<>();
            Path currentPath = MutablePath.make();
            for (Emit<E> emit : emits) {
                this.toEmit = emit;
                if (!emit.isFake()) {
                    currentPath = currentPath.extend(emit.getElement(), emit.getLabels());
                    emitComparators.add(this.toEmit.getSqlgComparatorHolder());
                }
            }
            if (this.toEmit != null) {
                this.toEmit.setPath(currentPath);
                this.toEmit.setSqlgComparatorHolders(emitComparators);
            }
        }
        return this.toEmit != null;
    }

    private void eagerLoad() {
        this.eagerLoadedResults.clear();
        while (flattenRawIterator()) {
            this.eagerLoadedResults.add(this.toEmit);
            if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
                this.toEmit.setRepeated(true);
                this.eagerLoadedResults.add(this.toEmit);
            }
            this.toEmit = null;
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.previousHead = null;
        this.eagerLoadedResults.clear();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    private Iterator<List<Emit<E>>> elements() {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = this.sqlgGraph.getGremlinParser().parse(this.replacedStepTree);
        SqlgCompiledResultIterator<List<Emit<E>>> resultIterator = new SqlgCompiledResultIterator<>(this.sqlgGraph, rootSchemaTableTrees);
        stopWatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("SqlgGraphStepCompiled finished, time taken {}", stopWatch.toString());
        }
        return resultIterator;
    }

    @Override
    public List<ReplacedStep<?, ?>> getReplacedSteps() {
        return this.replacedSteps;
    }

    @Override
    public ReplacedStepTree.TreeNode addReplacedStep(ReplacedStep<?, ?> replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size());
        this.replacedSteps.add(replacedStep);

        //New way of interpreting steps
        if (this.replacedStepTree == null) {
            //the first root node
            this.replacedStepTree = new ReplacedStepTree(replacedStep);
        } else {
            this.replacedStepTree.addReplacedStep(replacedStep);
        }
        return this.replacedStepTree.getCurrentTreeNodeNode();
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
                rootSchemaTableTree.resetColumnAliasMaps();
            }
        }
    }

    public boolean isForMultipleQueries() {
        return this.parsedForStrategySql.size() > 1 || this.parsedForStrategySql.values().stream().filter(l -> l.size() > 1).count() > 0;
    }


    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.returnClass.hashCode();
        for (final Object id : this.ids) {
            result ^= id.hashCode();
        }
        return result;
    }

}
