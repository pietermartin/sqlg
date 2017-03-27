package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.Arrays;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/04
 */
public class VertexStrategy extends BaseStrategy {

    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class, EdgeVertexStep.class, EdgeOtherVertexStep.class);
    private Logger logger = LoggerFactory.getLogger(VertexStrategy.class.getName());

    public VertexStrategy(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }


    public static VertexStrategy from(Traversal.Admin<?, ?> traversal) {
        return new VertexStrategy(traversal);
    }

    public void apply() {
        final Step<?, ?> startStep = this.traversal.getStartStep();

        //Only optimize graph step.
        if (!(this.traversal.getGraph().get() instanceof SqlgGraph)) {
            return;
        }

//        final GraphStep originalGraphStep = (GraphStep) startStep;

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInNormalBatchMode()) {
            this.sqlgGraph.tx().flush();
        }

//        if (originalGraphStep.getIds().length > 0) {
//            Object id = originalGraphStep.getIds()[0];
//            if (id != null) {
//                Class clazz = id.getClass();
//                if (!Stream.of(originalGraphStep.getIds()).allMatch(i -> clazz.isAssignableFrom(i.getClass())))
//                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
//            }
//        }
        if (this.canNotBeOptimized()) {
            this.logger.debug("gremlin not optimized due to path or tree step. " + this.traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
            return;
        }
        combineSteps();

    }

    @Override
    protected SqlgStep constructSqlgStep(Step startStep) {
        SqlgVertexStepCompiled sqlgStep = new SqlgVertexStepCompiled(this.traversal);
        ReplacedStep replacedStep = ReplacedStep.from(this.sqlgGraph.getTopology());
        sqlgStep.addReplacedStep(replacedStep);
        return sqlgStep;
    }

    @Override
    protected boolean isReplaceableStep(Class<? extends Step> stepClass, boolean alreadyReplacedGraphStep) {
        return CONSECUTIVE_STEPS_TO_REPLACE.contains(stepClass);
    }

    @Override
    protected void replaceStepInTraversal(Step stepToReplace, SqlgStep sqlgStep) {
        if (this.traversal.getSteps().contains(stepToReplace)) {
            TraversalHelper.replaceStep(stepToReplace, sqlgStep, this.traversal);
        } else {
            TraversalHelper.insertAfterStep(sqlgStep, stepToReplace.getPreviousStep(), this.traversal);
        }
    }
}
