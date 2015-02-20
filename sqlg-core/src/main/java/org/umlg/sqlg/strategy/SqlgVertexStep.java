package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.graph.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.Reversible;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.structure.SqlgVertex;

import java.util.*;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements Reversible {

    public final List<HasContainer> hasContainers = new ArrayList<>();

    public String[] edgeLabels;
    public Direction direction;
    public Class<E> returnClass;

    public SqlgVertexStep(final Traversal.Admin traversal, final Class<E> returnClass, final Direction direction, final String... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.returnClass = returnClass;
    }

    public String toString() {
        return edgeLabels.length > 0 ?
                TraversalHelper.makeStepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase()) :
                TraversalHelper.makeStepString(this, this.direction, this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        if (Vertex.class.isAssignableFrom(this.returnClass)) {
            if (this.hasContainers.isEmpty()) {
                return (Iterator<E>) ((SqlgVertex) traverser.get()).vertices(Collections.emptyList(), this.direction, this.edgeLabels);
            } else {
                return (Iterator<E>) ((SqlgVertex) traverser.get()).vertices(hasContainers, this.direction, this.edgeLabels);
            }
        } else {
            return (Iterator<E>) traverser.get().iterators().edgeIterator(this.direction, this.edgeLabels);
        }
    }
}
