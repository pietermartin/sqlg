package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;

import java.util.List;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/08/26
 */
public class SqlgAddVertexStartStep extends SqlgAbstractStep<Vertex, Vertex> implements Mutating<Event.VertexAddedEvent>, TraversalParent, Parameterizing, Scoping {

    private Parameters parameters;
    private boolean first = true;
    private CallbackRegistry<Event.VertexAddedEvent> callbackRegistry;

    public SqlgAddVertexStartStep(final Traversal.Admin traversal, Parameters parameters) {
        super(traversal);
        this.parameters = parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(this, keyValues);
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.parameters.getReferencedLabels();
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() {
        if (this.first) {
            this.first = false;
            final SqlgTraverserGenerator generator = SqlgTraverserGenerator.instance();
            final Vertex vertex = this.getTraversal().getGraph().get().addVertex(
                    this.parameters.getKeyValues(
                            generator.generate(false, (Step)this, 1L, false, false)
                    ));
            if (this.callbackRegistry != null && !this.callbackRegistry.getCallbacks().isEmpty()) {
                final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                final Event.VertexAddedEvent vae = new Event.VertexAddedEvent(eventStrategy.detach(vertex));
                this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
            }
            return generator.generate(vertex, (Step)this, 1L, false, false);
        } else
            throw FastNoSuchElementException.instance();
    }

    @Override
    public CallbackRegistry<Event.VertexAddedEvent> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.parameters.hashCode();
    }

    @Override
    public void reset() {
        super.reset();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.parameters);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.parameters.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public SqlgAddVertexStartStep clone() {
        final SqlgAddVertexStartStep clone = (SqlgAddVertexStartStep) super.clone();
        clone.parameters = this.parameters.clone();
        return clone;
    }
}
