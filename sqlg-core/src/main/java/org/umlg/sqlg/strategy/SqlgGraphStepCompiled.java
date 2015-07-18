package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.StreamFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Date: 2015/02/20
 * Time: 9:54 PM
 */
public class SqlgGraphStepCompiled<E extends Element> extends GraphStep<E> {

    private List<HasContainer> hasContainers = new ArrayList<>();
    private List<Pair<VertexStep<E>, List<HasContainer>>> replacedSteps = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(SqlgGraphStepCompiled.class.getName());
    private SqlgGraph sqlgGraph;

    public SqlgGraphStepCompiled(final GraphStep<E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.getIds());
        if (!originalGraphStep.getLabels().isEmpty()) {
            originalGraphStep.getLabels().forEach(this::addLabel);
        }

        this.sqlgGraph = (SqlgGraph)originalGraphStep.getTraversal().getGraph().get();
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    public void addReplacedStep(Pair<VertexStep<E>, List<HasContainer>> stepPair) {
        this.replacedSteps.add(stepPair);
    }
    public void addHasContainer(HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    private Iterator<? extends Edge> edges() {
        return null;
    }

    private Iterator<? extends Vertex> vertices() {
        Stream<? extends Vertex> vertexStream;
        if (this.ids != null && this.ids.length > 0) {
            //if ids are given assume it to be the most significant narrowing of vertices.
            //i.e. has logic will be done in memory.
            vertexStream = getVertices();
            //TODO support all Compares
        } else if (!this.replacedSteps.isEmpty()) {
//            return (Iterator<? extends Vertex>) internalGetElements();
            return null;
        } else {
            vertexStream = getVertices();
        }
//        return vertexStream.filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).iterator();
        return null;
    }

//    private Iterator<Element> internalGetElements() {
//        //the first replaced step must be a GraphStep, this step itself
//        HasContainer startHasContainer =  this.hasContainers.remove(0);
//        SchemaTable startSchemaTable = SqlgUtil.parseLabelMaybeNoSchema(startHasContainer.key);
//
//        List<Element> elements = new ArrayList<>();
//        startSchemaTable = SchemaTable.of(startSchemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + startSchemaTable.getTable());
//        SchemaTableTree schemaTableTree = this.sqlgGraph.getGremlinParser().parse(startSchemaTable, this.hasContainers, replacedSteps);
//        List<Triple<LinkedList<SchemaTableTree>, SchemaTable, String>> sqlStatements = schemaTableTree.constructSql();
//        for (Triple<LinkedList<SchemaTableTree>, SchemaTable, String> sqlTriple : sqlStatements) {
//            Connection conn = this.sqlgGraph.tx().getConnection();
//            if (logger.isDebugEnabled()) {
//                logger.debug(sqlTriple.getRight());
//            }
//            try (PreparedStatement preparedStatement = conn.prepareStatement(sqlTriple.getRight())) {
//                preparedStatement.setLong(1, this.primaryKey);
//                setParametersOnStatement(sqlTriple.getLeft(), conn, preparedStatement);
//                ResultSet resultSet = preparedStatement.executeQuery();
//                while (resultSet.next()) {
//                    this.sqlgGraph.getGremlinParser().loadElements(resultSet, sqlTriple.getMiddle(), elements);
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        return elements.iterator();
//    }

    private Stream<? extends Vertex> getVertices() {
        return StreamFactory.stream(this.sqlgGraph.vertices(this.ids));
    }
}
