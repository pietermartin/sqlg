package org.umlg.sqlg.step;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.strategy.SqlgComparatorHolder;
import org.umlg.sqlg.structure.SqlgCompiledResultIterator;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgTraverserGenerator;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.*;

/**
 * Date: 2015/02/20
 * Time: 9:54 PM
 */
public class SqlgGraphStep<S, E extends SqlgElement> extends GraphStep implements SqlgStep, TraversalParent {

    private Logger logger = LoggerFactory.getLogger(SqlgGraphStep.class.getName());
    private SqlgGraph sqlgGraph;

    private List<ReplacedStep<?, ?>> replacedSteps = new ArrayList<>();
    private ReplacedStepTree replacedStepTree;

    private Emit<E> toEmit = null;
    private Iterator<List<Emit<E>>> elementIter;

    private List<Emit<E>> traversers = new ArrayList<>();
    private ListIterator<Emit<E>> traversersLstIter;

    private Traverser.Admin<S> previousHead;

    private ReplacedStep<?, ?> lastReplacedStep;
    private long rangeCount = 0;
    private boolean eagerLoad = false;
    private boolean isForMultipleQueries = false;

    /**
     * This is a jippo of sorts.
     * Sqlg always uses SqlgTraverser which extends B_LP_O_P_S_SE_SL_Traverser.
     * This means the path is included in hashCode and equals which buggers up ...sack() gremlins.
     * So for traversals with a sack this indicates to ignore the path in the hasCode and equals.
     */
    private boolean requiresSack;
    private boolean requiresOneBulk;

    public SqlgGraphStep(final SqlgGraph sqlgGraph, final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final Object... ids) {
        super(traversal, returnClass, isStart, ids);
        this.sqlgGraph = sqlgGraph;
        this.requiresSack = TraversalHelper.hasStepOfAssignableClass(SackStep.class, traversal);
        this.requiresOneBulk = SqlgTraversalUtil.hasOneBulkRequirement(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (this.traversersLstIter != null && this.traversersLstIter.hasNext()) {
                Emit<E> emit = this.traversersLstIter.next();
                this.labels = emit.getLabels();
                if (applyRange(emit)) {
                    continue;
                }
                return emit.getTraverser();
            }
            if (!this.eagerLoad && (this.elementIter != null)) {
                if (this.elementIter.hasNext()) {
                    this.traversers.clear();
                    internalLoad();
                    this.traversersLstIter = this.traversers.listIterator();
                }
            }
            if (this.traversersLstIter != null && this.traversersLstIter.hasNext()) {
                Emit<E> emit = this.traversersLstIter.next();
                this.labels = emit.getLabels();
                if (applyRange(emit)) {
                    continue;
                }
                return emit.getTraverser();
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
                if (this.eagerLoad) {
                    eagerLoad();
                    Collections.sort(this.traversers);
                    this.traversersLstIter = this.traversers.listIterator();
                }
                this.lastReplacedStep = this.replacedSteps.get(this.replacedSteps.size() - 1);
            }
        }
    }

    private boolean applyRange(Emit<E> emit) {
        if (this.lastReplacedStep.hasRange() && this.lastReplacedStep.applyInStep() && this.lastReplacedStep.getDepth() == emit.getReplacedStepDepth()) {
            if (this.lastReplacedStep.getSqlgRangeHolder().getRange().isBefore(this.rangeCount + 1)) {
                throw FastNoSuchElementException.instance();
            }
            if (this.lastReplacedStep.getSqlgRangeHolder().getRange().isAfter(this.rangeCount)) {
                this.rangeCount++;
                return true;
            }
            this.rangeCount++;
        }
        return false;
    }

    public void setEagerLoad(boolean eager) {
        this.eagerLoad = eager;
    }

    public boolean isEargerLoad() {
        return this.eagerLoad;
    }

    //B_LP_O_P_S_SE_SL_Traverser
    private void eagerLoad() {
        this.traversers.clear();
        while (this.elementIter.hasNext()) {
            internalLoad();
        }
    }

    private void internalLoad() {
        List<Emit<E>> emits = this.elementIter.next();
        Traverser.Admin<E> traverser = null;
        boolean first = true;
        List<SqlgComparatorHolder> emitComparators = new ArrayList<>();
        for (Emit<E> emit : emits) {
            if (!emit.isFake()) {
                this.toEmit = emit;
                E e = emit.getElement();
                this.labels = emit.getLabels();
                if (!isStart && previousHead != null && traverser == null) {
                    traverser = previousHead.split(e, this);
                } else if (first) {
                    first = false;
                    traverser = SqlgTraverserGenerator.instance().generate(e, this, 1L, this.requiresSack, this.requiresOneBulk);
                } else {
                    traverser = traverser.split(e, this);
                }
                emitComparators.add(this.toEmit.getSqlgComparatorHolder());
            }
        }
        this.toEmit.setSqlgComparatorHolders(emitComparators);
        this.toEmit.setTraverser(traverser);
        this.toEmit.evaluateElementValueTraversal(0, traverser);
        this.traversers.add(this.toEmit);
        if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
            this.toEmit.setRepeated(true);
            this.traversers.add(this.toEmit);
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.previousHead = null;
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
            logger.debug("SqlgGraphStep finished, time taken {}", stopWatch.toString());
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

    public void parseForStrategy() {
        this.isForMultipleQueries = false;
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = this.sqlgGraph.getGremlinParser().parseForStrategy(this.replacedSteps);
        if (rootSchemaTableTrees.size() > 1) {
            this.isForMultipleQueries = true;
            for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
                rootSchemaTableTree.resetColumnAliasMaps();
            }
        } else {
            for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
                try {
                    //TODO this really sucks, constructsql should not query, but alas it does for P.within and temp table jol
                    if (this.sqlgGraph.tx().isOpen() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
                        throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
                    }
                    //Regular
                    List<LinkedList<SchemaTableTree>> distinctQueries = rootSchemaTableTree.constructDistinctQueries();
//                    //Optional
//                    List<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> leftJoinResult = new ArrayList<>();
//                    SchemaTableTree.constructDistinctOptionalQueries(rootSchemaTableTree, leftJoinResult);
//                    //Emit
//                    List<LinkedList<SchemaTableTree>> leftJoinResultEmit = new ArrayList<>();
//                    SchemaTableTree.constructDistinctEmitBeforeQueries(rootSchemaTableTree, leftJoinResultEmit);
//                    this.isForMultipleQueries = (distinctQueries.size() + leftJoinResult.size() + leftJoinResultEmit.size()) > 1;
                    this.isForMultipleQueries = (distinctQueries.size()) > 1;
                    if (this.isForMultipleQueries) {
                        break;
                    }
                } finally {
                    rootSchemaTableTree.resetColumnAliasMaps();
                }
            }
        }
    }

    @Override
    public boolean isForMultipleQueries() {
        return this.isForMultipleQueries;
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
