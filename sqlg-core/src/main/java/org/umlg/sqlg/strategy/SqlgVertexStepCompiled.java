package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.structure.SqlgVertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStepCompiled<E extends Element> extends FlatMapStep<Vertex, E> {

    private List<Pair<VertexStep, List<HasContainer>>> replacedSteps = new ArrayList<>();

    public SqlgVertexStepCompiled(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        return (Iterator<E>) ((SqlgVertex) traverser.get()).elements(Collections.unmodifiableList(this.replacedSteps));
    }

    void addReplacedStep(Pair<VertexStep, List<HasContainer>> stepPair) {
        this.replacedSteps.add(Pair.of(stepPair.getLeft(), Collections.unmodifiableList(new ArrayList<>(stepPair.getRight()))));
    }

    public List<Pair<VertexStep, List<HasContainer>>> getReplacedSteps() {
        return Collections.unmodifiableList(replacedSteps);
    }

}
