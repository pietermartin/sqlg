package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStepCompiled<S extends SqlgElement, E extends SqlgElement> extends FlatMapStep<S, E> {

    private List<ReplacedStep<S, E>> replacedSteps = new ArrayList<>();

    public SqlgVertexStepCompiled(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<S> traverser) {
        return traverser.get().elements(Collections.unmodifiableList(this.replacedSteps));
    }

    void addReplacedStep(ReplacedStep<S, E> replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size() + 1);
        this.replacedSteps.add(replacedStep);
    }

    public List<ReplacedStep<S, E>> getReplacedSteps() {
        return Collections.unmodifiableList(replacedSteps);
    }

}
