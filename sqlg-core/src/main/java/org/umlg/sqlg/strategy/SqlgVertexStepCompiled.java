package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStepCompiled<E extends SqlgElement> extends FlatMapStep implements SqlgStep {

    private Traverser.Admin<E> head = null;

    private List<ReplacedStep<?, ?>> replacedSteps = new ArrayList<>();
    private ReplacedStepTree replacedStepTree;

    private Map<SchemaTableTree, List<Pair<LinkedList<SchemaTableTree>, String>>> parsedForStrategySql = new HashMap<>();

    private Emit<E> toEmit = null;
    private Iterator<List<Emit<E>>> elementIter;

    private List<Emit<E>> traversers = new ArrayList<>();
    private ListIterator<Emit<E>> traversersLstIter;

    private ReplacedStep<?, ?> lastReplacedStep;
    private long rangeCount = 0;
    private boolean eagerLoad = false;


    SqlgVertexStepCompiled(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (this.traversersLstIter != null && this.traversersLstIter.hasNext()) {
                if (this.eagerLoad && this.lastReplacedStep.hasRange()) {
                    if (this.lastReplacedStep.getSqlgRangeHolder().getRange().isBefore(this.rangeCount + 1)) {
                        throw FastNoSuchElementException.instance();
                    }
                    if (this.lastReplacedStep.getSqlgRangeHolder().getRange().isAfter(this.rangeCount)) {
                        this.rangeCount++;
                        this.traversersLstIter.next();
                        continue;
                    }
                    this.rangeCount++;
                }
                Emit<E> emit = this.traversersLstIter.next();
                this.labels = emit.getLabels();
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
                if (this.eagerLoad && this.lastReplacedStep.hasRange()) {
                    if (this.lastReplacedStep.getSqlgRangeHolder().getRange().isBefore(this.rangeCount + 1)) {
                        throw FastNoSuchElementException.instance();
                    }
                    if (this.lastReplacedStep.getSqlgRangeHolder().getRange().isAfter(this.rangeCount)) {
                        this.rangeCount++;
                        this.traversersLstIter.next();
                        continue;
                    }
                    this.rangeCount++;
                }
                Emit<E> emit = this.traversersLstIter.next();
                this.labels = emit.getLabels();
                return emit.getTraverser();
            } else {
                this.head = this.starts.next();
                this.elementIter = flatMapCustom(this.head);
                if (this.eagerLoad) {
                    eagerLoad();
                    Collections.sort(this.traversers);
                    this.traversersLstIter = this.traversers.listIterator();
                }
                this.lastReplacedStep = this.replacedSteps.get(this.replacedSteps.size() - 1);
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
        Traverser.Admin<E> traverser = this.head;
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
        this.toEmit.setTraverser(traverser);
        this.toEmit.evaluateElementValueTraversal();
        this.traversers.add(this.toEmit);
        if (this.toEmit.isRepeat() && !this.toEmit.isRepeated()) {
            this.toEmit.setRepeated(true);
            this.traversers.add(this.toEmit);
        }
    }

    private Iterator<List<Emit<E>>> flatMapCustom(Traverser.Admin<E> traverser) {
        //for the OrderGlobalStep we'll need to remove the step here
        E s = traverser.get();

        SqlgGraph sqlgGraph = (SqlgGraph) s.graph();
        //If the order is over multiple tables then the resultSet will be completely loaded into memory and then sorted.
        this.replacedStepTree.maybeAddLabelToLeafNodes();
        //If the order is over multiple tables then the resultSet will be completely loaded into memory and then sorted.
        if (this.replacedStepTree.hasOrderBy()) {
            parseForStrategy(sqlgGraph, SchemaTable.of(s.getSchema(), s instanceof Vertex ? SchemaManager.VERTEX_PREFIX + s.getTable() : SchemaManager.EDGE_PREFIX + s.getTable()));
            if (!isForMultipleQueries() && this.replacedStepTree.orderByIsOrder()) {
                this.replacedStepTree.applyComparatorsOnDb();
            } else {
                setEagerLoad(true);
            }
        }
        //If a range follows an order that needs to be done in memory then do not apply the range on the db.
        if (this.replacedStepTree.hasRange()) {
            parseForStrategy(sqlgGraph, SchemaTable.of(s.getSchema(), s instanceof Vertex ? SchemaManager.VERTEX_PREFIX + s.getTable() : SchemaManager.EDGE_PREFIX + s.getTable()));
            if (!isForMultipleQueries() && this.replacedStepTree.orderByIsOrder()) {
            } else {
                this.replacedStepTree.doNotApplyRangeOnDb();
                setEagerLoad(true);
            }
        }

        return s.elements(this.replacedSteps);
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
    public void reset() {
        super.reset();
        this.head = null;
        this.parsedForStrategySql.clear();
        this.toEmit = null;
        this.elementIter = null;
        this.traversers.clear();
        this.traversersLstIter = null;
        this.lastReplacedStep = null;
        this.rangeCount = 0;
        this.eagerLoad = false;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    private void parseForStrategy(SqlgGraph sqlgGraph, SchemaTable schemaTable) {
        this.parsedForStrategySql.clear();
        Preconditions.checkState(this.replacedSteps.size() > 1, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(1).isVertexStep() || this.replacedSteps.get(1).isEdgeVertexStep()
                , "The first step must a VertexStep, EdgeVertexStep or GraphStep found " + this.replacedSteps.get(1).getStep().getClass().toString());
        SchemaTableTree rootSchemaTableTree = null;
        try {
            rootSchemaTableTree = sqlgGraph.getGremlinParser().parse(schemaTable, this.replacedSteps);
            List<Pair<LinkedList<SchemaTableTree>, String>> sqlStatements = rootSchemaTableTree.constructSql();
            this.parsedForStrategySql.put(rootSchemaTableTree, sqlStatements);
        } finally {
            if (rootSchemaTableTree != null)
                rootSchemaTableTree.resetColumnAliasMaps();
        }
    }

    @Override
    public boolean isForMultipleQueries() {
        return this.parsedForStrategySql.size() > 1 || this.parsedForStrategySql.values().stream().filter(l -> l.size() > 1).count() > 0;
    }

    @Override
    public void setEagerLoad(boolean eager) {
        this.eagerLoad = eager;
    }

    @Override
    public boolean isEargerLoad() {
        return this.eagerLoad;
    }

}
