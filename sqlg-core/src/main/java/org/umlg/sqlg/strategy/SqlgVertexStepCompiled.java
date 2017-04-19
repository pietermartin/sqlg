package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.*;

import java.util.*;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStepCompiled<E extends SqlgElement> extends FlatMapStep implements SqlgStep {

    private SqlgGraph sqlgGraph;

    private Map<SchemaTable, List<Traverser.Admin<E>>> heads = new LinkedHashMap<>();
    //This needs to be a Multimap as the same element, i.e. same RecordId, can come in on different paths.
    private LinkedListMultimap<RecordId, Traverser.Admin<E>> recordIdAdminMultimap = LinkedListMultimap.create();
    //    private Map<RecordId, Traverser.Admin<E>> recordIdHeads = new LinkedHashMap<>();
    private Map<SchemaTable, Iterator<List<Emit<E>>>> schemaTableElements = new LinkedHashMap<>();
    private Map<SchemaTable, List<Long>> schemaTableParentIds = new LinkedHashMap<>();

    private List<ReplacedStep<?, ?>> replacedSteps = new ArrayList<>();
    private ReplacedStepTree replacedStepTree;

    private Emit<E> toEmit = null;
    private Iterator<List<Emit<E>>> elementIter;

    private List<Emit<E>> traversers = new ArrayList<>();
    private ListIterator<Emit<E>> traversersLstIter;

    private ReplacedStep<?, ?> lastReplacedStep;
    private long rangeCount = 0;
    private boolean eagerLoad = false;
    private boolean isForMultipleQueries = false;


    SqlgVertexStepCompiled(final Traversal.Admin traversal) {
        super(traversal);
        this.sqlgGraph = (SqlgGraph) traversal.getGraph().get();
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
                return emit.getTraversers().remove(0);
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
                return emit.getTraversers().remove(0);
            } else {
                Iterator<Map.Entry<SchemaTable, Iterator<List<Emit<E>>>>> schemaTableIteratorEntry = this.schemaTableElements.entrySet().iterator();
                if (schemaTableIteratorEntry.hasNext()) {
                    this.elementIter = schemaTableIteratorEntry.next().getValue();
                    schemaTableIteratorEntry.remove();
                    if (this.eagerLoad) {
                        eagerLoad();
                        Collections.sort(this.traversers);
                        this.traversersLstIter = this.traversers.listIterator();
                    }
                    this.lastReplacedStep = this.replacedSteps.get(this.replacedSteps.size() - 1);
                } else {
                    barrierTheHeads();
                    flatMapCustom();
                }
            }
        }
    }

    private void barrierTheHeads() {
        //RepeatStep does not seem to call reset() so reset here.
        this.heads.clear();
//        this.recordIdHeads.clear();
        this.recordIdAdminMultimap.clear();
        this.schemaTableParentIds.clear();
        if (!this.starts.hasNext()) {
            throw FastNoSuchElementException.instance();
        }
        while (this.starts.hasNext()) {
            Traverser.Admin<E> h = this.starts.next();
            E value = h.get();
            SchemaTable schemaTable = value.getSchemaTablePrefixed();
            List<Traverser.Admin<E>> traverserList = this.heads.computeIfAbsent(schemaTable, k -> new ArrayList<>());
            traverserList.add(h);
            this.recordIdAdminMultimap.put((RecordId) value.id(), h);
//            this.recordIdHeads.put((RecordId) value.id(), h);
            List<Long> parentIdList = this.schemaTableParentIds.computeIfAbsent(schemaTable, k -> new ArrayList<>());
            if (!parentIdList.contains(((RecordId) value.id()).getId())) {
                parentIdList.add(((RecordId) value.id()).getId());
            }
        }
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
        Emit<E> emitToGetEmit = emits.get(0);
        List<Traverser.Admin<E>> heads = this.recordIdAdminMultimap.get(emitToGetEmit.getParent());
        for (Traverser.Admin<E> head : heads) {
            Preconditions.checkState(head != null, "head not found for " + emits.get(0).getParent().toString());
            Traverser.Admin<E> traverser = head;
            List<SqlgComparatorHolder> emitComparators = new ArrayList<>();
            for (Emit<E> emit : emits) {
                if (!emit.isFake()) {
                    if (emit.isIncomingOnlyLocalOptionalStep()) {
                        this.toEmit = emit;
                        break;
                    }
                    this.toEmit = emit;
                    E e = emit.getElement();
                    this.labels = emit.getLabels();
                    traverser = traverser.split(e, this);
                    emitComparators.add(this.toEmit.getSqlgComparatorHolder());
                } else {
                    this.toEmit = emit;
                }
            }
            this.toEmit.setSqlgComparatorHolders(emitComparators);
            this.toEmit.addTraverser(traverser);
            this.toEmit.evaluateElementValueTraversal(head.path().size(), traverser);
            this.traversers.add(this.toEmit);
            if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
                this.toEmit.setRepeated(true);
                this.toEmit.duplicateTraverser();
                this.traversers.add(this.toEmit);
            }
        }
    }

    private void flatMapCustom() {
        for (SchemaTable schemaTable : this.heads.keySet()) {
            parseForStrategy(schemaTable);
            this.replacedStepTree.maybeAddLabelToLeafNodes();
            //If the order is over multiple tables then the resultSet will be completely loaded into memory and then sorted.
            if (this.replacedStepTree.hasOrderBy()) {
                if (!isForMultipleQueries() && this.replacedStepTree.orderByIsOrder() && !this.replacedStepTree.orderByHasSelectOneStepAndForLabelNotInTree()) {
                    this.replacedStepTree.applyComparatorsOnDb();
                } else {
                    setEagerLoad(true);
                }
            }
            //If a range follows an order that needs to be done in memory then do not apply the range on the db.
            //range is always the last step as sqlg does not optimize beyond a range step.
            if (replacedStepTree.hasRange()) {
                if (replacedStepTree.hasOrderBy()) {
                    replacedStepTree.doNotApplyRangeOnDb();
                    setEagerLoad(true);
                } else {
                    if (!isForMultipleQueries()) {
                        //In this case the range is only applied on the db.
                        replacedStepTree.doNotApplyInStep();
                    }
                }
            }
            this.schemaTableElements.put(schemaTable, elements(schemaTable));
        }
    }

    /**
     * Called from SqlgVertexStepCompiler which compiled VertexStep and HasSteps.
     * This is only called when not in BatchMode
     */
    private Iterator<List<Emit<E>>> elements(SchemaTable schemaTable) {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        SchemaTableTree rootSchemaTableTree = sqlgGraph.getGremlinParser().parse(schemaTable, this.replacedSteps);
        rootSchemaTableTree.setParentIds(this.schemaTableParentIds.get(schemaTable));
        Set<SchemaTableTree> rootSchemaTableTrees = new HashSet<>();
        rootSchemaTableTrees.add(rootSchemaTableTree);
        return new SqlgCompiledResultIterator<>(this.sqlgGraph, rootSchemaTableTrees, true);
    }


    @Override
    protected Iterator<E> flatMap(final Traverser.Admin traverser) {
        throw new IllegalStateException("SqlgVertexStepCompiled.flatMap should never be called, it existVertexLabel been replaced with flatMapCustom");
    }

    @Override
    public ReplacedStepTree.TreeNode addReplacedStep(ReplacedStep replacedStep) {
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


    //This is only used in tests, think about, delete?
    public List<ReplacedStep<?, ?>> getReplacedSteps() {
        return this.replacedSteps;
    }

    @Override
    public SqlgVertexStepCompiled<E> clone() {
        final SqlgVertexStepCompiled<E> clone = (SqlgVertexStepCompiled<E>) super.clone();
        clone.heads = new LinkedHashMap<>();
//        clone.recordIdHeads = new LinkedHashMap<>();
        clone.recordIdAdminMultimap = LinkedListMultimap.create();
        this.schemaTableElements = new LinkedHashMap<>();
        clone.schemaTableParentIds = new LinkedHashMap<>();
        clone.traversers = new ArrayList<>();
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.heads.clear();
//        this.recordIdHeads.clear();
        this.recordIdAdminMultimap.clear();
        this.schemaTableElements.clear();
        this.schemaTableParentIds.clear();
        this.toEmit = null;
        this.elementIter = null;
        this.traversers.clear();
        this.traversersLstIter = null;
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

    private void parseForStrategy(SchemaTable schemaTable) {
        this.isForMultipleQueries = false;
        Preconditions.checkState(this.replacedSteps.size() > 1, "There must be at least one replacedStep");
        Preconditions.checkState(
                this.replacedSteps.get(1).isVertexStep() ||
                        this.replacedSteps.get(1).isEdgeVertexStep() ||
                        this.replacedSteps.get(1).isEdgeOtherVertexStep()
                , "The first step must a VertexStep, EdgeVertexStep, EdgeOtherVertexStep or GraphStep, found " + this.replacedSteps.get(1).getStep().getClass().toString());
        SchemaTableTree rootSchemaTableTree = null;
        try {
            rootSchemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, this.replacedSteps);
            //Regular
            List<LinkedList<SchemaTableTree>> distinctQueries = rootSchemaTableTree.constructDistinctQueries();
            //Optional
            List<Pair<LinkedList<SchemaTableTree>, Set<SchemaTableTree>>> leftJoinResult = new ArrayList<>();
            SchemaTableTree.constructDistinctOptionalQueries(rootSchemaTableTree, leftJoinResult);
            //Emit
            List<LinkedList<SchemaTableTree>> leftJoinResultEmit = new ArrayList<>();
            SchemaTableTree.constructDistinctEmitBeforeQueries(rootSchemaTableTree, leftJoinResultEmit);
            this.isForMultipleQueries = (distinctQueries.size() + leftJoinResult.size() + leftJoinResultEmit.size()) > 1;
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
    public boolean isEargerLoad() {
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
