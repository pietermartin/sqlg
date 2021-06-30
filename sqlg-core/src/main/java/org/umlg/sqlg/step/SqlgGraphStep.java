package org.umlg.sqlg.step;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.strategy.SqlgComparatorHolder;
import org.umlg.sqlg.structure.SqlgCompiledResultIterator;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.*;

/**
 * Date: 2015/02/20
 * Time: 9:54 PM
 */
@SuppressWarnings("unchecked")
public class SqlgGraphStep<S, E extends SqlgElement> extends GraphStep implements SqlgStep, TraversalParent {

    private final SqlgGraph sqlgGraph;

    private final List<ReplacedStep<?, ?>> replacedSteps = new ArrayList<>();
    private ReplacedStepTree replacedStepTree;

    private Emit<E> toEmit = null;
    private Iterator<List<Emit<E>>> elementIter;

    private final List<Emit<E>> traversers = new ArrayList<>();
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
    private final boolean requiresSack;
    private final boolean requiresOneBulk;

    @SuppressWarnings("unchecked")
    public SqlgGraphStep(final SqlgGraph sqlgGraph, final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final Object... ids) {
        super(traversal, returnClass, isStart, ids);
        this.sqlgGraph = sqlgGraph;
        this.requiresSack = TraversalHelper.hasStepOfAssignableClass(SackStep.class, traversal);
        this.requiresOneBulk = SqlgTraversalUtil.hasOneBulkRequirement(traversal);
    }

    @SuppressWarnings("unchecked")
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
            if (this.lastReplacedStep.getSqlgRangeHolder().hasRange()) {
                if (this.lastReplacedStep.getSqlgRangeHolder().getRange().isBefore(this.rangeCount + 1)) {
                    throw FastNoSuchElementException.instance();
                }
                if (this.lastReplacedStep.getSqlgRangeHolder().getRange().isAfter(this.rangeCount)) {
                    this.rangeCount++;
                    return true;
                }
            } else {
                Preconditions.checkState(this.lastReplacedStep.getSqlgRangeHolder().hasSkip(), "If not a range query then it must be a skip.");
                if (this.rangeCount < this.lastReplacedStep.getSqlgRangeHolder().getSkip()) {
                    this.rangeCount++;
                    return true;
                }
            }
            this.rangeCount++;
        }
        return false;
    }

    @Override
    public void setEagerLoad(boolean eager) {
        this.eagerLoad = eager;
    }

    @Override
    public boolean isEagerLoad() {
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
                    first = false;
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
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS, TraverserRequirement.ONE_BULK);
    }

    private Iterator<List<Emit<E>>> elements() {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = prepare();
        return new SqlgCompiledResultIterator<>(this.sqlgGraph, rootSchemaTableTrees);
    }

    private Set<SchemaTableTree> prepare() {
        this.replacedStepTree.maybeAddLabelToLeafNodes();
        Set<SchemaTableTree> rootSchemaTableTrees = parseForStrategy();
        //If the order is over multiple tables then the resultSet will be completely loaded into memory and then sorted.
        if (this.replacedStepTree.hasOrderBy()) {
            if (isForMultipleQueries() || !this.replacedStepTree.orderByIsOrder() || this.replacedStepTree.orderByIsBeforeLeftJoin()) {
                setEagerLoad(true);
                //Remove the dbComparators
                for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
                    rootSchemaTableTree.removeDbComparators();
                    rootSchemaTableTree.loadEager();
                }
            } else {
                //This is only needed for test assertions at the moment.
                this.replacedStepTree.applyComparatorsOnDb();
            }
        }
        //If a range follows an order that needs to be done in memory then do not apply the range on the db.
        //range is always the last step as sqlg does not optimize beyond a range step.
        if (this.replacedStepTree.hasRange()) {
            if (this.replacedStepTree.hasOrderBy()) {
                if (isForMultipleQueries()) {
                    this.replacedStepTree.doNotApplyRangeOnDb();
                    setEagerLoad(true);
                } else {
                    this.replacedStepTree.doNotApplyInStep();
                }
            } else {
                if (!isForMultipleQueries()) {
                    //In this case the range is only applied on the db.
                    this.replacedStepTree.doNotApplyInStep();
                } else {
                    this.replacedStepTree.doNotApplyRangeOnDb();
                }
            }
        }
        return rootSchemaTableTrees;
    }

    @Override
    public List<ReplacedStep<?, ?>> getReplacedSteps() {
        return this.replacedSteps;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public ReplacedStepTree.TreeNode addReplacedStep(ReplacedStep<?, ?> replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size());
        this.replacedSteps.add(replacedStep);
        if (this.replacedStepTree == null) {
            //the first root node
            this.replacedStepTree = new ReplacedStepTree(replacedStep);
        } else {
            this.replacedStepTree.addReplacedStep(replacedStep);
        }
        return this.replacedStepTree.getCurrentTreeNodeNode();
    }

    private Set<SchemaTableTree> parseForStrategy() {
        this.isForMultipleQueries = false;
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = this.sqlgGraph.getGremlinParser().parse(this.replacedStepTree);
        if (rootSchemaTableTrees.size() > 1) {
            this.isForMultipleQueries = true;
            for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
                rootSchemaTableTree.resetColumnAliasMaps();
            }
        } else {
            for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
                try {
                    //TODO this really sucks, constructsql should not query, but alas it does for P.within and temp table jol
                    if (this.sqlgGraph.tx().isOpen() && this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
                        throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
                    }
                    //Regular
                    List<LinkedList<SchemaTableTree>> distinctQueries = rootSchemaTableTree.constructDistinctQueries();
                    this.isForMultipleQueries = distinctQueries.size() > 1;
                    if (this.isForMultipleQueries) {
                        break;
                    }
                } finally {
                    rootSchemaTableTree.resetColumnAliasMaps();
                }
            }
        }
        return rootSchemaTableTrees;
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
