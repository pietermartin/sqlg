package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.umlg.sqlg.process.SqlGraphStepWithPathTraverser;
import org.umlg.sqlg.process.SqlgLabelledPathTraverser;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.*;

import java.util.*;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStepCompiled<S extends SqlgElement, E extends SqlgElement> extends FlatMapStep implements SqlgStep {

    private Traverser.Admin<S> head = null;
    private Traverser.Admin<S> originalHead = null;
    private Iterator<Pair<E, Multimap<String, Emit<E>>>> iterator = EmptyIterator.instance();
    private List<ReplacedStep<S, E>> replacedSteps = new ArrayList<>();
    private Map<SchemaTableTree, List<Pair<LinkedList<SchemaTableTree>, String>>> parsedForStrategySql = new HashMap<>();
    private List<EmitTree<E>> rootEmitTrees = new ArrayList<>();
    private EmitTree<E> currentEmitTree;
    private Pair<E, Multimap<String, Emit<E>>> next;
    private SqlGraphStepWithPathTraverser<E, E> sqlGraphStepWithPathTraverser;

    public SqlgVertexStepCompiled(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            if (this.currentEmitTree != null || this.iterator.hasNext()) {

                this.originalHead = (Traverser.Admin<S>) this.head.clone();
                Preconditions.checkState(this.originalHead instanceof SqlgLabelledPathTraverser);
                if (this.currentEmitTree == null) {
                    this.next = this.iterator.next();
                    E e = next.getLeft();
                    Multimap<String, Emit<E>> labeledObjects = next.getRight();
                    this.sqlGraphStepWithPathTraverser = (SqlGraphStepWithPathTraverser<E, E>) this.head;
                    this.sqlGraphStepWithPathTraverser.customSplit(e, ImmutablePath.make(), labeledObjects);
                    if (e == null) {
                        this.sqlGraphStepWithPathTraverser.set((E)new Dummy());
                    } else {
                        this.sqlGraphStepWithPathTraverser.set(e);
                    }
                }
                this.head = this.originalHead;

                Iterator<Emit> toEmit = sqlGraphStepWithPathTraverser.getToEmit().iterator();
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
                            (!rootEmitTreeContains(this.rootEmitTrees, emit) && !this.currentEmitTree.hasChild(emit.getElementPlusEdgeId()))) {

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
                    //Do not emit the last element if it has already been emitted.
                    if (this.currentEmitTree != null) {
//                        this.currentEmitTree = this.currentEmitTree.addEmit(-1, new Emit((E) sqlGraphStepWithPathTraverser.get(), Optional.empty(), false));
                        this.currentEmitTree = null;
                        return sqlGraphStepWithPathTraverser;
                    } else {
                        return sqlGraphStepWithPathTraverser;
                    }
                }
//                //each iteration is a new row. i.e. must start from the original head containing the the start element.
//                this.originalHead = (Traverser.Admin<S>) sqlGraphStepWithPathTraverser.clone();
//                sqlGraphStepWithPathTraverser.customSplit(e, this.head.path(), labeledObjects);
//                sqlGraphStepWithPathTraverser.set(e);
//                this.head = this.originalHead;
//                return sqlGraphStepWithPathTraverser;
            } else {
                this.head = this.starts.next();
                this.originalHead = (Traverser.Admin<S>) head.clone();
                this.iterator = this.flatMapCustom(this.head);
            }
        }
    }

    @Override
    public Set<String> getLabels() {
        return new HashSet<>();
    }

    protected Iterator<Pair<E, Multimap<String, Emit<E>>>> flatMapCustom(Traverser.Admin<S> traverser) {
        //for the OrderGlobalStep we'll need to remove the step here
        S s = traverser.get();
        SqlgGraph sqlgGraph = (SqlgGraph) s.graph();
        List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
        ListIterator<Step> stepIterator = steps.listIterator();
        boolean afterThis = false;
        while (stepIterator.hasNext()) {
            Step step = stepIterator.next();
            //only interested what happens after this step
            if (afterThis) {
                parseForStrategy(sqlgGraph, SchemaTable.of(s.getSchema(), s instanceof Vertex ? SchemaManager.VERTEX_PREFIX + s.getTable() : SchemaManager.EDGE_PREFIX + s.getTable()));
                if (!isForMultipleQueries()) {
                    if (step instanceof SqlgOrderGlobalStep) {
                        ((SqlgOrderGlobalStep) step).setIgnore(true);
                    }
                }
            }

            if (step == this) {
                afterThis = true;
            }
        }
        return s.elements(this.replacedSteps);
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin traverser) {
        throw new IllegalStateException("SqlgVertexStepCompiled.flatMap should never be called, it has been replaced with flatMapCustom");
    }

    @Override
    public void addReplacedStep(ReplacedStep replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size() + 1);
        this.replacedSteps.add(replacedStep);
    }


    //This is only used in tests, think about, delete?
    public List<ReplacedStep<S, E>> getReplacedSteps() {
        return replacedSteps;
    }

    @Override
    public void reset() {
        super.reset();
        this.iterator = EmptyIterator.instance();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.PATH);
    }

    @Override
    public void parseForStrategy() {

    }

    public void parseForStrategy(SqlgGraph sqlgGraph, SchemaTable schemaTable) {
        this.parsedForStrategySql.clear();
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(
                this.replacedSteps.get(0).isVertexStep() ||
                        this.replacedSteps.get(0).isEdgeVertexStep()
                , "The first step must a VertexStep or EdgeVertexStep found " + this.replacedSteps.get(0).getStep().getClass().toString());
        SchemaTableTree schemaTableTree = null;
        try {
            schemaTableTree = sqlgGraph.getGremlinParser().parse(schemaTable, this.replacedSteps);
            List<Pair<LinkedList<SchemaTableTree>, String>> sqlStatements = schemaTableTree.constructSql();
            this.parsedForStrategySql.put(schemaTableTree, sqlStatements);
        } finally {
            if (schemaTableTree != null)
                schemaTableTree.resetThreadVars();
        }
    }

    @Override
    public boolean isForMultipleQueries() {
        return this.parsedForStrategySql.size() > 1 || this.parsedForStrategySql.values().stream().filter(l -> l.size() > 1).count() > 0;
    }

}
