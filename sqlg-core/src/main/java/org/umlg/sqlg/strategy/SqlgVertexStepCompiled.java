package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.umlg.sqlg.process.SqlgRawIteratorToEmitIterator;
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
    private Iterator<Emit<E>> iterator = EmptyIterator.instance();
    private List<ReplacedStep<?, ?>> replacedSteps = new ArrayList<>();
    private Map<SchemaTableTree, List<Pair<LinkedList<SchemaTableTree>, String>>> parsedForStrategySql = new HashMap<>();
    private ReplacedStepTree replacedStepTree;

    public SqlgVertexStepCompiled(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {

                Traverser.Admin<E> traverser = this.head;
                Emit<E> emit = this.iterator.next();
                //fake is true for local step repeat emit queries.
                //fake indicates that the incoming traverser element needs to be emitted.
                //It is fake as no query was executed to get the element seeing as its already on the traverser.
                if (emit.isFake()) {
                    return traverser;
                } else {
                    //if the step is a local step and it existVertexLabel no label then it is for the incoming object and only and already on the traverser.
                    if (emit.isIncomingOnlyLocalOptionalStep()) {
                        return traverser;
                    } else {
                        for (int i = 0; i < emit.getPath().size(); i++) {
                            E e = (E) emit.getPath().objects().get(i);
                            traverser = traverser.split(e, EmptyStep.instance());
                            traverser.addLabels(emit.getPath().labels().get(i));
                        }
                    }
                    return traverser;
                }

            } else {
                this.head = this.starts.next();
                this.iterator = new SqlgRawIteratorToEmitIterator<>(this.flatMapCustom(this.head));
            }
        }
    }

    @Override
    public Set<String> getLabels() {
        return new HashSet<>();
    }

    private Iterator<List<Emit<E>>> flatMapCustom(Traverser.Admin<E> traverser) {
        //for the OrderGlobalStep we'll need to remove the step here
        E s = traverser.get();
        SqlgGraph sqlgGraph = (SqlgGraph) s.graph();
        List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
        ListIterator<Step> stepIterator = steps.listIterator();
        boolean afterThis = false;
        while (stepIterator.hasNext()) {
            Step step = stepIterator.next();
            //only interested what happens after this step
            if (afterThis) {
                if (step instanceof SqlgOrderGlobalStep) {
                    parseForStrategy(sqlgGraph, SchemaTable.of(s.getSchema(), s instanceof Vertex ? SchemaManager.VERTEX_PREFIX + s.getTable() : SchemaManager.EDGE_PREFIX + s.getTable()));
                    if (!isForMultipleQueries()) {
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
        this.iterator = EmptyIterator.instance();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public void parseForStrategy() {

    }

    private void parseForStrategy(SqlgGraph sqlgGraph, SchemaTable schemaTable) {
        this.parsedForStrategySql.clear();
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isVertexStep() || this.replacedSteps.get(0).isEdgeVertexStep() || this.replacedSteps.get(0).isGraphStep()
                , "The first step must a VertexStep, EdgeVertexStep or GraphStep found " + this.replacedSteps.get(0).getStep().getClass().toString());
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

}
