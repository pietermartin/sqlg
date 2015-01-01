package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;

import java.util.List;

/**
 * Date: 2014/08/15
 * Time: 8:10 PM
 */
public class SqlgHasStep extends FilterStep<Element> implements HasContainerHolder, Reversible {

    private HasStep hasStep;

    public SqlgHasStep(Traversal traversal, final HasStep hasStep) {
        super(traversal);
        this.hasStep = hasStep;
        this.setPredicate(
                traverser -> new SqlgHasContainer((HasContainer)this.hasStep.getHasContainers().get(0)).test(traverser.get())
        );
        this.setLabel(this.hasStep.getLabel());
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.hasStep.getHasContainers());
    }

    public HasContainer getHasContainer() {
        return (HasContainer)this.hasStep.getHasContainers().get(0);
    }

    public List<HasContainer> getHasContainers() {
        return this.hasStep.getHasContainers();
    }
}
