package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
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
public class SqlgVertexStep<E extends Element> extends VertexStep<E> {

    public final List<HasContainer> hasContainers = new ArrayList<>();

    public SqlgVertexStep(final Traversal.Admin traversal, final Class<E> returnClass, final Direction direction, final String... edgeLabels) {
        super(traversal, returnClass, direction, edgeLabels);
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        if (Vertex.class.isAssignableFrom(getReturnClass())) {
            if (this.hasContainers.isEmpty()) {
                return (Iterator<E>) ((SqlgVertex) traverser.get()).vertices(Collections.emptyList(), getDirection(), getEdgeLabels());
            } else {
                return (Iterator<E>) ((SqlgVertex) traverser.get()).vertices(hasContainers, getDirection(), getEdgeLabels());
            }
        } else {
            return (Iterator<E>) traverser.get().edges(getDirection(), getEdgeLabels());
        }
    }

}
