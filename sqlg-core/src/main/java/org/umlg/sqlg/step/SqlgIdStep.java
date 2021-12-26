package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Element;

public class SqlgIdStep extends SqlgMapStep<Element, Object> implements TraversalParent {
    
    public SqlgIdStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Object map(Traverser.Admin<Element> traverser) {
        return traverser.get().id();
    }
}
