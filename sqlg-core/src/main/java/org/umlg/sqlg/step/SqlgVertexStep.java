package org.umlg.sqlg.step;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.step.barrier.SqlgLocalStepBarrier;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.strategy.SqlgComparatorHolder;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.traverser.ISqlgTraverser;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2014/08/15
 */
public class SqlgVertexStep<E extends SqlgElement> extends SqlgAbstractStep implements SqlgStep {

    private static final Logger logger = LoggerFactory.getLogger(SqlgVertexStep.class);
    private final SqlgGraph sqlgGraph;

    //This holds the head/start traversers per SchemaTable.
    //A query is executed per SchemaTable
    private Map<SchemaTable, List<Traverser.Admin<E>>> heads = new LinkedHashMap<>();

    //Holds all the incoming head/start elements start index. i.e. the index in which it was added to starts.
    private LinkedHashMap<Long, Traverser.Admin<E>> startIndexTraverserAdminMap = new LinkedHashMap<>();
    //important to start with 1 here to distinguish is from the default long being 0
    private long startIndex = 1;

    /**
     * Needs to be a multi map as TinkerPop will add new starts mid traversal.
     * look at TestRepeatStepVertexOut#testUntilRepeat
     */
    private LinkedListMultimap<SchemaTable, ListIterator<List<Emit<E>>>> schemaTableElements = LinkedListMultimap.create();

    //This holds, for each SchemaTable, a list of RecordId's ids and the start elements' index.
    //It is used to generate the select statements, 'VALUES' and ORDER BY 'index' sql
    private Map<SchemaTable, List<Pair<RecordId.ID, Long>>> schemaTableParentIds = new LinkedHashMap<>();

    private final List<ReplacedStep<?, ?>> replacedSteps = new ArrayList<>();
    private ReplacedStepTree replacedStepTree;

    private Emit<E> toEmit = null;
    private ListIterator<List<Emit<E>>> elementIterator;

    private List<Emit<E>> traversers = new ArrayList<>();
    private ListIterator<Emit<E>> traversersListIterator;

    private ReplacedStep<?, ?> lastReplacedStep;
    private long rangeCount = 0;
    private boolean eagerLoad = false;
    private boolean isForMultipleQueries = false;
    private boolean hasAggregateFunction;

    private Traverser.Admin<E> currentHead;
    private boolean isSqlgLocalStepBarrierChild;
    private boolean first = true;
    private boolean hasStarts = false;

    public SqlgVertexStep(final Traversal.Admin traversal) {
        super(traversal);
        this.sqlgGraph = (SqlgGraph) traversal.getGraph().get();
        this.isSqlgLocalStepBarrierChild = traversal.getParent() instanceof SqlgLocalStepBarrier;
    }

    @Override
    public boolean hasStarts() {
        if (this.first) {
            this.hasStarts = this.starts.hasNext();
        }
        return this.hasStarts;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        this.first = false;
        if (this.starts.hasNext()) {
            barrierTheHeads();
            constructQueryPerSchemaTable();
        }
        while (true) {
            if (this.traversersListIterator != null && this.traversersListIterator.hasNext()) {
                Emit<E> emit = this.traversersListIterator.next();
                this.labels = emit.getLabels();
                if (applyRange(emit)) {
                    continue;
                }
                return emit.getTraverser();
            }
            if (!this.eagerLoad && (this.elementIterator != null)) {
                if (this.elementIterator.hasNext()) {
                    this.traversers.clear();
                    this.traversersListIterator = internalLoad();
                }
            }
            if (this.traversersListIterator != null && this.traversersListIterator.hasNext()) {
                Emit<E> emit = this.traversersListIterator.next();
                this.labels = emit.getLabels();
                if (applyRange(emit)) {
                    continue;
                }
                return emit.getTraverser();
            } else {
                Iterator<Map.Entry<SchemaTable, ListIterator<List<Emit<E>>>>> schemaTableIteratorEntry = this.schemaTableElements.entries().iterator();
                if (schemaTableIteratorEntry.hasNext()) {
                    this.elementIterator = schemaTableIteratorEntry.next().getValue();
                    schemaTableIteratorEntry.remove();
                    if (this.eagerLoad) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("eager load is true");
                        }
                        eagerLoad();
                        Collections.sort(this.traversers);
                        this.traversersListIterator = this.traversers.listIterator();
                    }
                    this.lastReplacedStep = this.replacedSteps.get(this.replacedSteps.size() - 1);
                } else {
                    if (!this.starts.hasNext()) {
                        throw FastNoSuchElementException.instance();
                    } else {
                        throw new IllegalStateException("BUG: this should never happen.");
                    }
                }
            }
        }
    }

    private void barrierTheHeads() {
        //these collections are only used for the current starts.
        this.heads.clear();
        this.schemaTableParentIds.clear();
        while (this.starts.hasNext()) {
            @SuppressWarnings("unchecked")
            Traverser.Admin<E> h = this.starts.next();
            E value = h.get();
            SchemaTable schemaTable = value.getSchemaTablePrefixed();
            List<Traverser.Admin<E>> traverserList = this.heads.get(schemaTable);
            //noinspection Java8MapApi
            if (traverserList == null) {
                traverserList = new ArrayList<>();
                this.heads.put(schemaTable, traverserList);
            }
            traverserList.add(h);
            List<Pair<RecordId.ID, Long>> parentIdList = this.schemaTableParentIds.get(schemaTable);
            //noinspection Java8MapApi
            if (parentIdList == null) {
                parentIdList = new ArrayList<>();
                this.schemaTableParentIds.put(schemaTable, parentIdList);
            }
            parentIdList.add(Pair.of(((RecordId) value.id()).getID(), this.startIndex));
            this.startIndexTraverserAdminMap.put(this.startIndex++, h);
        }
    }

    //B_LP_O_P_S_SE_SL_Traverser
    private void eagerLoad() {
        this.traversers.clear();
        while (this.elementIterator.hasNext()) {
            //can ignore the result as the result gets sorted before the iterator is set.
            internalLoad();
        }
    }

    private ListIterator<Emit<E>> internalLoad() {
        List<Emit<E>> emits = this.elementIterator.next();
        if (this.isSqlgLocalStepBarrierChild || !this.hasAggregateFunction) {
            Emit<E> emitToGetEmit = emits.get(0);
            Traverser.Admin<E> head = this.startIndexTraverserAdminMap.get(emitToGetEmit.getParentIndex());
            if (head == null) {
                throw new IllegalStateException("head not found for " + emits.get(0).toString());
            }

            if (this.currentHead != null && !this.currentHead.equals(head)) {
                for (Object step : getTraversal().getSteps()) {
                    if ((step instanceof SqlgGroupStep)) {
                        ((Step<?, ?>)step).reset();
                    }
                }
            }
            this.currentHead = head;

            Traverser.Admin<E> traverser = head;
            List<SqlgComparatorHolder> emitComparators = new ArrayList<>();
            for (Emit<E> emit : emits) {
                emit.getElement().setInternalStartTraverserIndex(emit.getParentIndex());
                if (!emit.isFake()) {
                    if (emit.isIncomingOnlyLocalOptionalStep()) {
                        //no split it happening for left joined elements
                        ((ISqlgTraverser) traverser).setStartElementIndex(emit.getParentIndex());
                        traverser.get().setInternalStartTraverserIndex(emit.getParentIndex());
                        this.toEmit = emit;
                        break;
                    }
                    this.toEmit = emit;
                    E e = emit.getElement();
                    this.labels = emit.getLabels();
                    //noinspection unchecked
                    traverser = traverser.split(e, this);
                    if (traverser instanceof ISqlgTraverser) {
                        ((ISqlgTraverser) traverser).setStartElementIndex(emit.getParentIndex());
                    }
                    emitComparators.add(this.toEmit.getSqlgComparatorHolder());
                } else {
                    this.toEmit = emit;
                }
            }
            this.toEmit.setSqlgComparatorHolders(emitComparators);
            this.toEmit.setTraverser(traverser);

            this.toEmit.evaluateElementValueTraversal(head.path().size(), traverser);
            this.traversers.add(this.toEmit);
            if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
                this.toEmit.setRepeated(true);
                this.traversers.add(this.toEmit);
            }
            return this.traversers.listIterator();
        } else {
            for (Emit<E> emit : emits) {
                Traverser.Admin<E> traverser = SqlgTraverserGenerator.instance().generate(emit.getElement(), this, 1L, false, false);
                emit.setTraverser(traverser);
                this.traversers.add(emit);
            }
            return this.traversers.listIterator();
        }
    }

    private void constructQueryPerSchemaTable() {
        for (SchemaTable schemaTable : this.heads.keySet()) {
            SchemaTableTree rootSchemaTableTree = parseForStrategy(schemaTable);

            //If the order is over multiple tables then the resultSet will be completely loaded into memory and then sorted.
            if (this.replacedStepTree.hasOrderBy()) {
                if (isForMultipleQueries() || !replacedStepTree.orderByIsOrder() || this.replacedStepTree.orderByHasSelectOneStepAndForLabelNotInTree()) {
                    setEagerLoad(true);
                    //Remove the dbComparators
                    rootSchemaTableTree.removeDbComparators();
                } else {
                    //This is only needed for test assertions at the moment.
                    replacedStepTree.applyComparatorsOnDb();
                }
            }

            //If a range follows an order that needs to be done in memory then do not apply the range on the db.
            //range is always the last step as sqlg does not optimize beyond a range step.
            if (this.replacedStepTree.hasRange()) {
                if (this.replacedStepTree.hasOrderBy()) {
                    this.replacedStepTree.doNotApplyRangeOnDb();
                    setEagerLoad(true);
                } else {
                    if (!isForMultipleQueries()) {
                        //In this case the range is only applied on the db.
                        this.replacedStepTree.doNotApplyInStep();
                    }
                }
            }
            this.schemaTableElements.put(schemaTable, elements(schemaTable, rootSchemaTableTree));
        }
    }

    /**
     * Called from SqlgVertexStepCompiler which compiled VertexStep and HasSteps.
     * This is only called when not in BatchMode
     */
    private ListIterator<List<Emit<E>>> elements(SchemaTable schemaTable, SchemaTableTree rootSchemaTableTree) {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        rootSchemaTableTree.setParentIdsAndIndexes(this.schemaTableParentIds.get(schemaTable));
        Set<SchemaTableTree> rootSchemaTableTrees = new HashSet<>();
        rootSchemaTableTrees.add(rootSchemaTableTree);
        return new SqlgCompiledResultListIterator<>(new SqlgCompiledResultIterator<>(this.sqlgGraph, rootSchemaTableTrees, true));
    }

    @Override
    public ReplacedStepTree.TreeNode addReplacedStep(ReplacedStep replacedStep) {
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


    //This is only used in tests, think about, delete?
    public List<ReplacedStep<?, ?>> getReplacedSteps() {
        return this.replacedSteps;
    }

    @Override
    public SqlgVertexStep<E> clone() {
        @SuppressWarnings("unchecked") final SqlgVertexStep<E> clone = (SqlgVertexStep<E>) super.clone();
        clone.heads = new LinkedHashMap<>();
        this.schemaTableElements = LinkedListMultimap.create();
        clone.schemaTableParentIds = new LinkedHashMap<>();
        clone.traversers = new ArrayList<>();
        clone.startIndexTraverserAdminMap = new LinkedHashMap<>();
        clone.startIndex = 1;
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
        this.startIndex = 1;
        this.heads.clear();
        this.startIndexTraverserAdminMap.clear();
        this.schemaTableElements.clear();
        this.schemaTableParentIds.clear();
        this.toEmit = null;
        this.elementIterator = null;
        this.traversers.clear();
        this.traversersListIterator = null;
        this.lastReplacedStep = null;
        this.rangeCount = 0;
        this.eagerLoad = false;
        this.isForMultipleQueries = false;
        this.replacedStepTree.reset();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    private SchemaTableTree parseForStrategy(SchemaTable schemaTable) {
        this.isForMultipleQueries = false;
        Preconditions.checkState(this.replacedSteps.size() > 1, "There must be at least one replacedStep");
        Preconditions.checkState(
                this.replacedSteps.get(1).isVertexStep() ||
                        this.replacedSteps.get(1).isEdgeVertexStep() ||
                        this.replacedSteps.get(1).isEdgeOtherVertexStep()
                , "The first step must a VertexStep, EdgeVertexStep, EdgeOtherVertexStep or GraphStep, found " + this.replacedSteps.get(1).getStep().getClass().toString());
        SchemaTableTree rootSchemaTableTree = null;
        try {
            rootSchemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, this.replacedStepTree, this.isSqlgLocalStepBarrierChild);
            //Regular
            List<LinkedList<SchemaTableTree>> distinctQueries = rootSchemaTableTree.constructDistinctQueries();
            //Optional
            List<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> leftJoinResult = new ArrayList<>();
            SchemaTableTree.constructDistinctOptionalQueries(rootSchemaTableTree, leftJoinResult);
            //Emit
            List<LinkedList<SchemaTableTree>> leftJoinResultEmit = new ArrayList<>();
            SchemaTableTree.constructDistinctEmitBeforeQueries(rootSchemaTableTree, leftJoinResultEmit);
            this.isForMultipleQueries = (distinctQueries.size() + leftJoinResult.size() + leftJoinResultEmit.size()) > 1;
            this.hasAggregateFunction = !distinctQueries.isEmpty() && distinctQueries.get(distinctQueries.size() - 1).getLast().hasAggregateFunction();
            this.hasAggregateFunction = this.hasAggregateFunction || (!leftJoinResult.isEmpty() && leftJoinResult.get(leftJoinResult.size() - 1).getLeft().getLast().hasAggregateFunction());
            this.hasAggregateFunction = this.hasAggregateFunction || (!leftJoinResultEmit.isEmpty() && leftJoinResultEmit.get(leftJoinResultEmit.size() - 1).getLast().hasAggregateFunction());
            return rootSchemaTableTree;
        } finally {
            if (rootSchemaTableTree != null) {
                rootSchemaTableTree.resetColumnAliasMaps();
            }
        }
    }

    @Override
    public boolean isForMultipleQueries() {
        return this.isForMultipleQueries;
    }

    @Override
    public void setEagerLoad(boolean eager) {
        this.eagerLoad = eager;
    }

    @Override
    public boolean isEagerLoad() {
        return this.eagerLoad;
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

}
