package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
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
        if (Vertex.class.isAssignableFrom(this.returnClass))
            this.setFunction(traverser -> {
                if (this.hasContainers.isEmpty()) {
                    return (Iterator<E>) ((SqlgVertex)traverser.get()).vertices(Collections.emptyList(), this.direction, this.edgeLabels);
                } else {
                    return (Iterator<E>) ((SqlgVertex)traverser.get()).vertices(hasContainers, this.direction, this.edgeLabels);
                }
            });
        else
            this.setFunction(traverser -> (Iterator<E>) traverser.get().iterators().edgeIterator(this.direction, this.edgeLabels));
    }

    public String toString() {
        return edgeLabels.length > 0 ?
                TraversalHelper.makeStepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase()) :
                TraversalHelper.makeStepString(this, this.direction, this.returnClass.getSimpleName().toLowerCase());
    }

}
