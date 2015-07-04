package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.*;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStepCompiled<S extends SqlgElement, E extends SqlgElement> extends FlatMapStep<S, E> {

    private B_O_P_S_SE_SL_Traverser head = null;
    private Iterator<Pair<E, Map<String, Object>>> iterator = EmptyIterator.instance();
    private List<ReplacedStep<S, E>> replacedSteps = new ArrayList<>();

    public SqlgVertexStepCompiled(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {
                Pair<E, Map<String, Object>> next = this.iterator.next();
                E e = next.getLeft();
                Map<String, Object> labeledObjects = next.getRight();
                //split before setting the path.
                //This is because the labels must be set on a unique path for every iteration.
                B_O_P_S_SE_SL_Traverser split = (B_O_P_S_SE_SL_Traverser) this.head.split(e, this);
                for (String label : labeledObjects.keySet()) {
                    split.setPath(split.path().extend(labeledObjects.get(label), new HashSet<>(Arrays.asList(label))));
                }
                return split;
            } else {
                this.head = (B_O_P_S_SE_SL_Traverser) this.starts.next();
                this.iterator = this.flatMapCustom(this.head);
            }
        }
    }

    @Override
    public Set<String> getLabels() {
        return Collections.EMPTY_SET;
    }

    protected Iterator<Pair<E, Map<String, Object>>> flatMapCustom(Traverser.Admin<S> traverser) {
        Iterator<Pair<E, Map<String, Object>>> iter = traverser.get().elements(Collections.unmodifiableList(this.replacedSteps));
        return iter;
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<S> traverser) {
        throw new IllegalStateException("SqlgVertexStepCompiled.flatMap should never be called, it has been replaced with flatMapCustom");
    }

    void addReplacedStep(ReplacedStep<S, E> replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size() + 1);
        this.replacedSteps.add(replacedStep);
    }

    public List<ReplacedStep<S, E>> getReplacedSteps() {
        return Collections.unmodifiableList(replacedSteps);
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
}
