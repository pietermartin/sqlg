package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.process.SqlgRawIteratorToEmitIterator;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.structure.SqlgCompiledResultIterator;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;
import java.util.function.Supplier;

/**
 * Date: 2015/02/20
 * Time: 9:54 PM
 */
public class SqlgGraphStepCompiled<S extends SqlgElement, E extends SqlgElement> extends GraphStep implements SqlgStep, TraversalParent {

    private Logger logger = LoggerFactory.getLogger(SqlgGraphStepCompiled.class.getName());

    private List<ReplacedStep<S, E>> replacedSteps = new ArrayList<>();
    private SqlgGraph sqlgGraph;
    private Map<SchemaTableTree, List<Pair<LinkedList<SchemaTableTree>, String>>> parsedForStrategySql = new HashMap<>();

    protected transient Supplier<Iterator<Emit<E>>> iteratorSupplier;
    private Iterator<Emit<E>> iterator = EmptyIterator.instance();
    private Traverser.Admin<E> head = null;


    public SqlgGraphStepCompiled(final SqlgGraph sqlgGraph, final Traversal.Admin traversal, final Class<S> returnClass, final boolean isStart, final Object... ids) {
        super(traversal, returnClass, isStart, ids);
        this.sqlgGraph = sqlgGraph;
        this.iteratorSupplier = new SqlgRawIteratorToEmitIterator<>(this::elements);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {
                Traverser.Admin<E> traverser;
                Emit<E> emit = this.iterator.next();
                E element = emit.getElement();

                if (emit.getPath() != null) {

                    //create a traverser for the first element
                    E e = (E) emit.getPath().objects().get(0);
                    if (this.isStart) {
                        traverser = this.getTraversal().getTraverserGenerator().generate(e, this, 1l);
                    } else {
                        traverser = this.head.split(e, this);
                    }
                    traverser.addLabels(emit.getPath().labels().get(0));

                    //create traversers for the rest of the elements, i.e. skipping the first
                    for (int i = 1; i < emit.getPath().size(); i++) {
                        e = (E) emit.getPath().objects().get(i);
                        traverser = traverser.split(e, EmptyStep.instance());
                        traverser.addLabels(emit.getPath().labels().get(i));
                    }

                } else {

                    if (this.isStart) {
                        traverser = this.getTraversal().getTraverserGenerator().generate(element, this, 1l);
                    } else {
                        traverser = this.head.split(element, this);
                    }

                }
                return traverser;
            } else {
                if (this.isStart) {
                    if (this.done)
                        throw FastNoSuchElementException.instance();
                    else {
                        this.done = true;
                        this.iterator = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
                    }
                } else {
                    this.head = this.starts.next();
                    this.iterator = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
                }
            }
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    private Iterator<Pair<E, Multimap<String, Emit<E>>>> elements() {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = this.sqlgGraph.getGremlinParser().parse(this.replacedSteps);
        SqlgCompiledResultIterator<Pair<E, Multimap<String, Emit<E>>>> resultIterator = new SqlgCompiledResultIterator<>(this.sqlgGraph, rootSchemaTableTrees);
        stopWatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("SqlgGraphStepCompiled finished, time taken {}", stopWatch.toString());
        }
        return resultIterator;
    }

    @Override
    public void addReplacedStep(ReplacedStep replacedStep) {
        //depth is + 1 because there is always a root node who's depth is 0
        replacedStep.setDepth(this.replacedSteps.size());
        this.replacedSteps.add(replacedStep);
    }

    @Override
    public void parseForStrategy() {
        this.parsedForStrategySql.clear();
        Preconditions.checkState(this.replacedSteps.size() > 0, "There must be at least one replacedStep");
        Preconditions.checkState(this.replacedSteps.get(0).isGraphStep(), "The first step must a SqlgGraphStep");
        Set<SchemaTableTree> rootSchemaTableTrees = this.sqlgGraph.getGremlinParser().parseForStrategy(this.replacedSteps);
        for (SchemaTableTree rootSchemaTableTree : rootSchemaTableTrees) {
            try {
                //TODO this really sucks, constructsql should not query, but alas it does for P.within and temp table jol
                if (this.sqlgGraph.tx().isOpen() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
                    throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
                }
                List<Pair<LinkedList<SchemaTableTree>, String>> sqlStatements = rootSchemaTableTree.constructSql();
                this.parsedForStrategySql.put(rootSchemaTableTree, sqlStatements);
            } finally {
                rootSchemaTableTree.resetThreadVars();
            }
        }
    }

    public boolean isForMultipleQueries() {
        return this.parsedForStrategySql.size() > 1 || this.parsedForStrategySql.values().stream().filter(l -> l.size() > 1).count() > 0;
    }

    public List<ReplacedStep<S, E>> getReplacedSteps() {
        return replacedSteps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SqlgGraphStepCompiled<?, ?> that = (SqlgGraphStepCompiled<?, ?>) o;

        if (logger != null ? !logger.equals(that.logger) : that.logger != null) return false;
        if (replacedSteps != null ? !replacedSteps.equals(that.replacedSteps) : that.replacedSteps != null)
            return false;
        if (sqlgGraph != null ? !sqlgGraph.equals(that.sqlgGraph) : that.sqlgGraph != null) return false;
        if (parsedForStrategySql != null ? !parsedForStrategySql.equals(that.parsedForStrategySql) : that.parsedForStrategySql != null)
            return false;
        if (iteratorSupplier != null ? !iteratorSupplier.equals(that.iteratorSupplier) : that.iteratorSupplier != null)
            return false;
        if (iterator != null ? !iterator.equals(that.iterator) : that.iterator != null) return false;
        return head != null ? head.equals(that.head) : that.head == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.returnClass.hashCode();
        for (final Object id : this.ids) {
            result ^= id.hashCode();
        }
        return result;
    }

}
