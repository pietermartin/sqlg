package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import org.umlg.sqlg.structure.SqlgVertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgVertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements Reversible {

    public final List<HasContainer> hasContainers = new ArrayList<>();

    public String[] edgeLabels;
    public Direction direction;
    public int branchFactor;
    public Class<E> returnClass;

    public SqlgVertexStep(final Traversal traversal, final Class<E> returnClass, final Direction direction, final int branchFactor, final String... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.branchFactor = branchFactor;
        this.returnClass = returnClass;
        if (Vertex.class.isAssignableFrom(this.returnClass))
            this.setFunction(traverser -> {
                if (this.hasContainers.isEmpty()) {
                    return (Iterator<E>) ((SqlgVertex)traverser.get()).vertices(Collections.emptyList(), this.direction, this.branchFactor, this.edgeLabels);
                } else {
                    return (Iterator<E>) ((SqlgVertex)traverser.get()).vertices(hasContainers, this.direction, this.branchFactor, this.edgeLabels);
                }
            });
        else
            this.setFunction(traverser -> (Iterator<E>) traverser.get().iterators().edges(this.direction, this.branchFactor, this.edgeLabels));
    }

}
