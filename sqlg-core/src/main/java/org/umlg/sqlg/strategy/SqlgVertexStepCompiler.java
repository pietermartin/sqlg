package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyVertex;
import org.umlg.sqlg.structure.SqlgVertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStepCompiler<E extends Element> extends FlatMapStep<Vertex, E> {

    private List<Pair<VertexStep, List<HasContainer>>> replacedSteps = new ArrayList<>();

    public SqlgVertexStepCompiler(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        Vertex v = traverser.get();
        if (v instanceof StrategyVertex) {
            return (Iterator<E>) ((SqlgVertex)((StrategyVertex) traverser.get()).getBaseVertex()).elements(this.replacedSteps);
        } else {
            return (Iterator<E>) ((SqlgVertex) traverser.get()).elements(this.replacedSteps);
        }
    }

    public void addReplacedStep(Pair<VertexStep, List<HasContainer>> stepPair) {
        this.replacedSteps.add(stepPair);
    }

    public List<Pair<VertexStep, List<HasContainer>>> getReplacedSteps() {
        return replacedSteps;
    }
}
