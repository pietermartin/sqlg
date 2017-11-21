package org.umlg.sqlg.step.barrier;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.umlg.sqlg.step.SqlgFilterStep;
import org.umlg.sqlg.strategy.SqlgSqlExecutor;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/19
 */
public class SqlgDropStepBarrier<S> extends SqlgFilterStep<S>  implements Mutating<Event> {

    private CallbackRegistry<Event> callbackRegistry;
    private SqlgGraph sqlgGraph;
    private boolean first = true;
    private Set<RecordId> idsToDelete = new HashSet<>();
    private MultiValuedMap<Pair<EdgeLabel, VertexLabel>, Long> foreignKeyOutEdgesToDelete = new HashSetValuedHashMap<>();
    private MultiValuedMap<Pair<EdgeLabel, VertexLabel>, Long> foreignKeyInEdgesToDelete = new HashSetValuedHashMap<>();
    private MultiValuedMap<EdgeLabel, Long> edgesToDelete = new HashSetValuedHashMap<>();
    private MultiValuedMap<VertexLabel, Long> verticesToDelete = new HashSetValuedHashMap<>();

    public SqlgDropStepBarrier(final Traversal.Admin traversal, CallbackRegistry<Event> callbackRegistry) {
        super(traversal);
        this.sqlgGraph = (SqlgGraph) traversal.getGraph().get();
        this.callbackRegistry = callbackRegistry;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        if (this.first) {
            this.first = false;
            EventStrategy eventStrategy = null;
            if (!this.callbackRegistry.getCallbacks().isEmpty()) {
                eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
            }
            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                Object object = start.get();
                if (object instanceof SqlgElement) {
                    SqlgElement sqlgElement = (SqlgElement) object;
                    RecordId recordId = (RecordId) sqlgElement.id();
                    SchemaTable schemaTable = recordId.getSchemaTable();
                    Long id = recordId.getId();
                    if (sqlgElement instanceof SqlgVertex) {
                        Optional<VertexLabel> vertexLabelOptional = this.sqlgGraph.getTopology().getVertexLabel(schemaTable.getSchema(), schemaTable.getTable());
                        Preconditions.checkState(vertexLabelOptional.isPresent());
                        boolean added = this.verticesToDelete.put(vertexLabelOptional.get(), id);
                        if (added && eventStrategy != null) {
                            final Event removeEvent = new Event.VertexRemovedEvent(eventStrategy.detach((SqlgVertex) sqlgElement));
                            this.callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
                        }
                        for (EdgeLabel outEdgeLabel : vertexLabelOptional.get().getOutEdgeLabels().values()) {
                            this.foreignKeyOutEdgesToDelete.put(Pair.of(outEdgeLabel, vertexLabelOptional.get()), id);
                        }
                        for (EdgeLabel inEdgeLabel : vertexLabelOptional.get().getInEdgeLabels().values()) {
                            this.foreignKeyInEdgesToDelete.put(Pair.of(inEdgeLabel, vertexLabelOptional.get()), id);
                        }
                    } else if (sqlgElement instanceof SqlgEdge) {
                        Optional<EdgeLabel> edgeLabelOptional = this.sqlgGraph.getTopology().getEdgeLabel(schemaTable.getSchema(), schemaTable.getTable());
                        Preconditions.checkState(edgeLabelOptional.isPresent());
                        boolean added = this.edgesToDelete.put(edgeLabelOptional.get(), id);
                        if (added && eventStrategy != null) {
                            final Event removeEvent = new Event.EdgeRemovedEvent(eventStrategy.detach((SqlgEdge) sqlgElement));
                            this.callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
                        }
                    }
                } else if (object instanceof SqlgProperty) {
                    SqlgProperty sqlgProperty = (SqlgProperty)object;
                    if (eventStrategy != null) {
                        final Event removeEvent = new Event.VertexPropertyRemovedEvent(eventStrategy.detach((SqlgVertexProperty) sqlgProperty));
                        callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
                    }
                    sqlgProperty.remove();
                } else {
                    throw new IllegalStateException("Expected SqlgElement or SqlgProperty. Found " + object.getClass().getSimpleName());
                }
            }
        }
        for (Pair<EdgeLabel, VertexLabel> edgeLabelVertexLabelPair : this.foreignKeyOutEdgesToDelete.keySet()) {
            EdgeLabel outEdgeLabel = edgeLabelVertexLabelPair.getKey();
            VertexLabel vertexLabel = edgeLabelVertexLabelPair.getValue();
            Collection<Long> ids = this.foreignKeyOutEdgesToDelete.get(edgeLabelVertexLabelPair);
            String sql = this.sqlgGraph.getSqlDialect().dropWithForeignKey(true, outEdgeLabel, vertexLabel, ids, !this.callbackRegistry.getCallbacks().isEmpty());
            SqlgSqlExecutor.executeDropEdges(this.sqlgGraph,  outEdgeLabel, sql, this.callbackRegistry.getCallbacks());
        }
        for (Pair<EdgeLabel, VertexLabel> edgeLabelVertexLabelPair : this.foreignKeyInEdgesToDelete.keySet()) {
            EdgeLabel inEdgeLabel = edgeLabelVertexLabelPair.getKey();
            VertexLabel vertexLabel = edgeLabelVertexLabelPair.getValue();
            Collection<Long> ids = this.foreignKeyInEdgesToDelete.get(edgeLabelVertexLabelPair);
            String sql = this.sqlgGraph.getSqlDialect().dropWithForeignKey(false, inEdgeLabel, vertexLabel, ids, !this.callbackRegistry.getCallbacks().isEmpty());
            SqlgSqlExecutor.executeDropEdges(this.sqlgGraph, inEdgeLabel, sql, this.callbackRegistry.getCallbacks());
        }
        for (EdgeLabel edgeLabel : this.edgesToDelete.keySet()) {
            Collection<Long> ids = this.edgesToDelete.get(edgeLabel);
            String sql = this.sqlgGraph.getSqlDialect().drop(edgeLabel, ids);
            SqlgSqlExecutor.executeDrop(this.sqlgGraph, sql);
        }
        for (VertexLabel vertexLabel : this.verticesToDelete.keySet()) {
            Collection<Long> ids = this.verticesToDelete.get(vertexLabel);
            String sql = this.sqlgGraph.getSqlDialect().drop(vertexLabel, ids);
            SqlgSqlExecutor.executeDrop(this.sqlgGraph, sql);
        }
        //The standard TraversalFilterStep.filter calls TraversalUtil.test which normally resets the traversal for every incoming start.
        reset();
        throw FastNoSuchElementException.instance();
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return false;
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
        this.idsToDelete.clear();
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    /**
     * This method doesn't do anything as {@code drop()} doesn't take property mutation arguments.
     */
    public void addPropertyMutations(final Object... keyValues) {
        // do nothing
    }
}
