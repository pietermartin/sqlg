package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.umlg.sqlgraph.sql.impl.SchemaCreator;
import org.umlg.sqlgraph.sql.impl.SchemaManager;
import org.umlg.sqlgraph.sql.impl.SqlGraphTransaction;

/**
 * Date: 2014/07/12
 * Time: 5:38 AM
 */
public class SqlGraph implements Graph {

    private final SqlGraphTransaction sqlGraphTransaction;

    public SqlGraph() {
        SchemaCreator.INSTANCE.setSqlGraph(this);
        this.sqlGraphTransaction = new SqlGraphTransaction(this);
        this.tx().readWrite();
        SchemaManager.INSTANCE.ensureGlobalVerticesTableExist();
        SchemaManager.INSTANCE.ensureGlobalEdgesTableExist();
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        this.tx().readWrite();
        SchemaManager.INSTANCE.ensureVertexTableExist(label, keyValues);
        final SqlVertex vertex = new SqlVertex(this, label, keyValues);
        return vertex;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        return null;
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return null;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C>... graphComputerClass) {
        return null;
    }

    @Override
    public SqlGraphTransaction tx() {
        return this.sqlGraphTransaction;
    }

    @Override
    public <V extends Variables> V variables() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

}
