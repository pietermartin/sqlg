package org.umlg.sqlg.step.barrier;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.umlg.sqlg.step.SqlgFilterStep;
import org.umlg.sqlg.strategy.SqlgSqlExecutor;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/19
 */
public class SqlgDropStepBarrier<S> extends SqlgFilterStep<S> implements Mutating<Event> {

    private CallbackRegistry<Event> callbackRegistry;
    private final SqlgGraph sqlgGraph;
    private boolean first = true;
    private final Parameters parameters = new Parameters();
    private final Set<RecordId> idsToDelete = new HashSet<>();
    private final MultiValuedMap<Pair<EdgeLabel, VertexLabel>, RecordId.ID> foreignKeyOutEdgesToDelete = new HashSetValuedHashMap<>();
    private final MultiValuedMap<Pair<EdgeLabel, VertexLabel>, RecordId.ID> foreignKeyInEdgesToDelete = new HashSetValuedHashMap<>();
    private final MultiValuedMap<EdgeLabel, RecordId.ID> edgesToDelete = new HashSetValuedHashMap<>();
    private final MultiValuedMap<VertexLabel, RecordId.ID> verticesToDelete = new HashSetValuedHashMap<>();

    public SqlgDropStepBarrier(final Traversal.Admin traversal, CallbackRegistry<Event> callbackRegistry) {
        super(traversal);
        this.sqlgGraph = (SqlgGraph) traversal.getGraph().get();
        this.callbackRegistry = callbackRegistry;
    }

    /**
     * This method doesn't do anything as {@code drop()} doesn't take property mutation arguments.
     */
    @Override
    public void configure(final Object... keyValues) {
        // Do nothing.
    }

    @Override
    public Parameters getParameters() {
        return parameters;
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
                    RecordId.ID id = recordId.getID();
                    if (sqlgElement instanceof SqlgVertex) {
                        Optional<VertexLabel> vertexLabelOptional = this.sqlgGraph.getTopology().getVertexLabel(schemaTable.getSchema(), schemaTable.getTable());
                        Preconditions.checkState(vertexLabelOptional.isPresent());
                        SqlgVertex sqlgVertex = (SqlgVertex) sqlgElement;
                        boolean added = this.verticesToDelete.put(vertexLabelOptional.get(), id);
                        if (added && eventStrategy != null) {
                            final Event removeEvent = new Event.VertexRemovedEvent(eventStrategy.detach(sqlgVertex));
                            this.callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
                        }
                        for (EdgeLabel outEdgeLabel : vertexLabelOptional.get().getOutEdgeLabels().values()) {
                            //If there are registered callBacks and the dialect does not support returning deleted rows we need to do it the slow way.
                            //Get all the edges, register to the callBack and delete.
                            if (eventStrategy != null) {
                                Iterator<Edge> edges = sqlgVertex.edges(Direction.OUT);
                                while (edges.hasNext()) {
                                    Edge edge = edges.next();
                                    SchemaTable schemaTableEdge = ((SqlgEdge) edge).getSchemaTablePrefixed().withOutPrefix();
                                    Optional<EdgeLabel> edgeLabelOptional = this.sqlgGraph.getTopology().getEdgeLabel(schemaTableEdge.getSchema(), schemaTableEdge.getTable());
                                    Preconditions.checkState(edgeLabelOptional.isPresent());
                                    added = this.edgesToDelete.put(edgeLabelOptional.get(), ((RecordId) edge.id()).getID());
                                    if (added) {
                                        final Event removeEvent = new Event.EdgeRemovedEvent(eventStrategy.detach(edge));
                                        this.callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
                                    }
                                }
                            } else {
                                this.foreignKeyOutEdgesToDelete.put(Pair.of(outEdgeLabel, vertexLabelOptional.get()), id);
                            }
                        }
                        for (EdgeLabel inEdgeLabel : vertexLabelOptional.get().getInEdgeLabels().values()) {
                            //If there are registered callBacks and the dialect does not support returning deleted rows we need to do it the slow way.
                            //Get all the edges, register to the callBack and delete.
                            if (!this.callbackRegistry.getCallbacks().isEmpty()) {
                                Iterator<Edge> edges = sqlgVertex.edges(Direction.IN);
                                while (edges.hasNext()) {
                                    Edge edge = edges.next();
                                    SchemaTable schemaTableEdge = ((SqlgEdge) edge).getSchemaTablePrefixed().withOutPrefix();
                                    Optional<EdgeLabel> edgeLabelOptional = this.sqlgGraph.getTopology().getEdgeLabel(schemaTableEdge.getSchema(), schemaTableEdge.getTable());
                                    Preconditions.checkState(edgeLabelOptional.isPresent());
                                    added = this.edgesToDelete.put(edgeLabelOptional.get(), ((RecordId) edge.id()).getID());
                                    if (added) {
                                        final Event removeEvent = new Event.EdgeRemovedEvent(eventStrategy.detach(edge));
                                        this.callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
                                    }
                                }
                            } else {
                                this.foreignKeyInEdgesToDelete.put(Pair.of(inEdgeLabel, vertexLabelOptional.get()), id);
                            }
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
                    SqlgProperty sqlgProperty = (SqlgProperty) object;
                    if (eventStrategy != null) {
                        final Event removeEvent;
                        if (sqlgProperty.element() instanceof Edge) {
                            removeEvent = new Event.EdgePropertyRemovedEvent(eventStrategy.detach((Edge) sqlgProperty.element()), eventStrategy.detach(sqlgProperty));
                        } else if (sqlgProperty instanceof VertexProperty)
                            removeEvent = new Event.VertexPropertyRemovedEvent(eventStrategy.detach((VertexProperty) sqlgProperty));
                        else
                            throw new IllegalStateException("The incoming object is not removable: " + object);
                        this.callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
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
            Collection<RecordId.ID> ids = this.foreignKeyOutEdgesToDelete.get(edgeLabelVertexLabelPair);
            String sql = this.sqlgGraph.getSqlDialect().dropWithForeignKey(true, outEdgeLabel, vertexLabel, ids, !this.callbackRegistry.getCallbacks().isEmpty());
            SqlgSqlExecutor.executeDropEdges(this.sqlgGraph, outEdgeLabel, sql, this.callbackRegistry.getCallbacks());
        }
        for (Pair<EdgeLabel, VertexLabel> edgeLabelVertexLabelPair : this.foreignKeyInEdgesToDelete.keySet()) {
            EdgeLabel inEdgeLabel = edgeLabelVertexLabelPair.getKey();
            VertexLabel vertexLabel = edgeLabelVertexLabelPair.getValue();
            Collection<RecordId.ID> ids = this.foreignKeyInEdgesToDelete.get(edgeLabelVertexLabelPair);
            String sql = this.sqlgGraph.getSqlDialect().dropWithForeignKey(false, inEdgeLabel, vertexLabel, ids, !this.callbackRegistry.getCallbacks().isEmpty());
            SqlgSqlExecutor.executeDropEdges(this.sqlgGraph, inEdgeLabel, sql, this.callbackRegistry.getCallbacks());
        }
        for (EdgeLabel edgeLabel : this.edgesToDelete.keySet()) {
            Collection<RecordId.ID> ids = this.edgesToDelete.get(edgeLabel);
            String sql = this.sqlgGraph.getSqlDialect().drop(edgeLabel, ids);
            SqlgSqlExecutor.executeDrop(this.sqlgGraph, sql);
        }
        for (VertexLabel vertexLabel : this.verticesToDelete.keySet()) {
            Collection<RecordId.ID> ids = this.verticesToDelete.get(vertexLabel);
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
}
