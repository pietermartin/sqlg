package org.umlg.sqlgraph.process.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.structure.Element;

/**
 * Date: 2014/07/12
 * Time: 5:49 AM
 */
public class SqlGraphStep<E extends Element> extends GraphStep<E> {

    public SqlGraphStep(Traversal traversal, Class<E> returnClass) {
        super(traversal, returnClass);
    }

    @Override
    public void generateTraverserIterator(boolean trackPaths) {

    }

    @Override
    public void clear() {

    }
}
