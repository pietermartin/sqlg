package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.*;
import org.apache.tinkerpop.gremlin.structure.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.predicate.*;
import org.umlg.sqlg.services.SqlgFunctionFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.services.SqlgPGVectorFactory;
import org.umlg.sqlg.sql.parse.*;
import org.umlg.sqlg.step.*;
import org.umlg.sqlg.step.barrier.*;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.AbstractLabel;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.util.SqlgTraversalUtil;
import org.umlg.sqlg.util.SqlgUtil;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.Compare.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Contains.within;
import static org.apache.tinkerpop.gremlin.process.traversal.Contains.without;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols.*;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/03/04
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseStrategy.class);

    static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(
            VertexStep.class,
            EdgeVertexStep.class,
            GraphStep.class,
            EdgeOtherVertexStep.class,
            OrderGlobalStep.class,
            RangeGlobalStep.class,
            ChooseStep.class,
            OptionalStep.class,
            RepeatStep.class,
            SelectStep.class,
            SelectOneStep.class,
            DropStep.class,
            PropertiesStep.class,
            PropertyMapStep.class,
            ElementMapStep.class,
            MaxGlobalStep.class,
            MinGlobalStep.class,
            SumGlobalStep.class,
            MeanGlobalStep.class,
            CountGlobalStep.class,
            GroupStep.class,
            GroupCountStep.class,
            IdStep.class,
            NotStep.class,
            CallStep.class
//            FoldStep.class
    );

    public static final String PATH_LABEL_SUFFIX = "P~~~";
    public static final String EMIT_LABEL_SUFFIX = "E~~~";
    public static final String SQLG_PATH_FAKE_LABEL = "sqlgPathFakeLabel";
    public static final String SQLG_PATH_TEMP_FAKE_LABEL = "sqlgPathTempFakeLabel";
    public static final String SQLG_PATH_ORDER_RANGE_LABEL = "sqlgPathOrderRangeLabel";
    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(
            eq, neq, gt, gte, lt, lte
    );
    public static final List<BiPredicate> SUPPORTED_LABEL_BI_PREDICATE = Arrays.asList(
            eq, neq, within, without
    );
    public static final List<BiPredicate> SUPPORTED_ID_BI_PREDICATE = Arrays.asList(
            eq, neq, within, without
    );

    final Traversal.Admin<?, ?> traversal;
    final SqlgGraph sqlgGraph;
    SqlgStep sqlgStep = null;
    private final Stack<ReplacedStepTree.TreeNode> optionalStepStack = new Stack<>();
    private final Stack<ReplacedStepTree.TreeNode> chooseStepStack = new Stack<>();
    ReplacedStepTree.TreeNode currentTreeNodeNode;
    ReplacedStep<?, ?> currentReplacedStep;
    /**
     * reset is used in {@link VertexStrategy#combineSteps()} where it allows the optimization to continue.
     */
    boolean reset = false;

    BaseStrategy(Traversal.Admin<?, ?> traversal) {
        this.traversal = traversal;
        Optional<Graph> graph = traversal.getGraph();
        Preconditions.checkState(graph.isPresent(), "BUG: SqlgGraph must be present on the traversal.");
        this.sqlgGraph = (SqlgGraph) graph.get();
    }

    abstract void combineSteps();

    /**
     * this.sqlgStep is either a {@link SqlgGraphStep} or {@link SqlgVertexStep}.
     *
     * @return false if optimization must be terminated.
     */
    boolean handleStep(ListIterator<Step<?, ?>> stepIterator, MutableInt pathCount) {
        Step<?, ?> step = stepIterator.next();
        removeTinkerPopLabels(step);
        if (step instanceof GraphStep) {
            doFirst(stepIterator, step, pathCount);
        } else if (this.sqlgStep == null) {
            boolean keepGoing = doFirst(stepIterator, step, pathCount);
            stepIterator.previous();
            return keepGoing;
        } else {
            if (step instanceof VertexStep || step instanceof EdgeVertexStep || step instanceof EdgeOtherVertexStep) {
                handleVertexStep(stepIterator, (AbstractStep<?, ?>) step, pathCount, false);
            } else if (step instanceof CallStep callStep) {
                handleCallStep(stepIterator, callStep, pathCount);
            } else if (step instanceof RepeatStep repeatStep) {
                List<RepeatStep> repeatSteps = TraversalHelper.getStepsOfAssignableClassRecursively(RepeatStep.class, this.traversal);
//                if (false && isRecursiveRepeatStep(repeatStep)) {
                if (isRecursiveRepeatStep(repeatStep)) {
                    handleRecursiveRepeatStep(repeatStep, pathCount, stepIterator);
                } else if (optimizableRepeatStep(repeatSteps)) {
                    handleRepeatStep(repeatStep, pathCount);
                } else {
                    this.currentReplacedStep.addLabel((pathCount) + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
                    return false;
                }
            } else if (step instanceof OptionalStep optionalStep) {
                if (!unoptimizableOptionalStep(optionalStep)) {
                    this.optionalStepStack.clear();
                    handleOptionalStep(1, optionalStep, this.traversal, pathCount);
                    this.optionalStepStack.clear();
                    //after choose steps the optimization starts over in VertexStrategy
                    this.reset = true;
                } else {
                    return false;
                }
            } else if (step instanceof ChooseStep chooseStep) {
                if (!unoptimizableChooseStep(chooseStep)) {
                    this.chooseStepStack.clear();
                    handleChooseStep(1, chooseStep, this.traversal, pathCount);
                    this.chooseStepStack.clear();
                    //after choose steps the optimization starts over
                    this.reset = true;
                } else {
                    return false;
                }
            } else if (step instanceof OrderGlobalStep) {
                stepIterator.previous();
                handleOrderGlobalSteps(stepIterator, pathCount);
                handleRangeGlobalSteps(stepIterator, pathCount);
            } else if (step instanceof RangeGlobalStep) {
                handleRangeGlobalSteps(stepIterator, pathCount);
            } else if (step instanceof SelectStep || (step instanceof SelectOneStep)) {
                handleOrderGlobalSteps(stepIterator, pathCount);
                handleRangeGlobalSteps(step, stepIterator, pathCount);
                //select step can not be followed by a PropertyStep
                if (step instanceof SelectOneStep selectOneStep) {
                    String key = (String) selectOneStep.getScopeKeys().iterator().next();
                    if (stepIterator.hasNext()) {
                        Step<?, ?> next = stepIterator.next();
                        if (next instanceof PropertiesStep) {
                            //get the step for the label
                            Optional<ReplacedStep<?, ?>> labeledReplacedStep = this.sqlgStep.getReplacedSteps().stream().filter(
                                    r -> {
                                        //Take the first
                                        if (r.hasLabels()) {
                                            String label = r.getLabels().iterator().next();
                                            String stepLabel = SqlgUtil.originalLabel(label);
                                            return stepLabel.equals(key);
                                        } else {
                                            return false;
                                        }
                                    }
                            ).findAny();
                            Preconditions.checkState(labeledReplacedStep.isPresent());
                            ReplacedStep<?, ?> replacedStep = labeledReplacedStep.get();
                            handlePropertiesStep(replacedStep, next);
                            return true;
                        } else {
                            stepIterator.previous();
                            return false;
                        }
                    }
                } else {
                    SelectStep<?, ?> selectStep = (SelectStep<?, ?>) step;
                    List<? extends Traversal.Admin<Object, ?>> children = selectStep.getLocalChildren();
                    if (children.size() == selectStep.getSelectKeys().size()) {
                        Optional<? extends Traversal.Admin<Object, ?>> traversalOpt = children.stream().filter(c -> c.getSteps().isEmpty() || !(c.getSteps().get(0) instanceof ElementMapStep)).findAny();
                        if (traversalOpt.isEmpty()) {
//                            TestElementMap.testElementMapSelectBy
//
//                            optimizes
//                            .select("a", "b")
//                            .by(__.elementMap("prop1"))
//                            .by(__.elementMap("prop2"));
                            int count = 0;
                            for (String selectKey : selectStep.getSelectKeys()) {
                                Traversal.Admin<Object, ?> traversal = children.get(count++);
                                ElementMapStep elementMapStep = (ElementMapStep) traversal.getSteps().get(0);

                                Optional<ReplacedStep<?, ?>> labeledReplacedStep = this.sqlgStep.getReplacedSteps().stream().filter(
                                        r -> {
                                            //Take the first
                                            if (r.hasLabels()) {
                                                String label = r.getLabels().iterator().next();
                                                String stepLabel = SqlgUtil.originalLabel(label);
                                                return stepLabel.equals(selectKey);
                                            } else {
                                                return false;
                                            }
                                        }
                                ).findAny();
                                Preconditions.checkState(labeledReplacedStep.isPresent());
                                ReplacedStep<?, ?> replacedStep = labeledReplacedStep.get();
                                handleElementMapStep(replacedStep, elementMapStep, traversal);
                            }
                        }
                    }

                }
            } else if (step instanceof DropStep && (!this.sqlgGraph.getSqlDialect().isMariaDb())) {
                Traversal.Admin<?, ?> root = TraversalHelper.getRootTraversal(this.traversal);
                final Optional<EventStrategy> eventStrategyOptional = root.getStrategies().getStrategy(EventStrategy.class);
                //noinspection StatementWithEmptyBody
                if (eventStrategyOptional.isEmpty()) {
                    //MariaDB does not support target and source together.
                    //Table 'E_ab' is specified twice, both as a target for 'DELETE' and as a separate source for data
                    //This has been fixed in 10.3.1, waiting for it to land in the repo.
                    handleDropStep();
                } else {
                    //Do nothing, it will go via the SqlgDropStepBarrier.
                }
                return false;
            } else if (step instanceof DropStep && this.sqlgGraph.getSqlDialect().isMariaDb()) {
                return false;
            } else if (step instanceof PropertiesStep) {
                return handlePropertiesStep(this.currentReplacedStep, step);
            } else if (step instanceof PropertyMapStep) {
                return handlePropertyMapStep(step);
            } else if (step instanceof ElementMapStep<?, ?>) {
                return handleElementMapStep(this.currentReplacedStep, step);
            } else if (step instanceof IdStep) {
                return handleIdStep(step);
            } else if (step instanceof MaxGlobalStep) {
                return handleAggregateGlobalStep(this.currentReplacedStep, step, max);
            } else if (step instanceof MinGlobalStep) {
                return handleAggregateGlobalStep(this.currentReplacedStep, step, min);
            } else if (step instanceof SumGlobalStep) {
                return handleAggregateGlobalStep(this.currentReplacedStep, step, sum);
            } else if (step instanceof MeanGlobalStep) {
                return handleAggregateGlobalStep(this.currentReplacedStep, step, "avg");
            } else if (step instanceof CountGlobalStep) {
                //this indicates that the count step is preceded with an `order.by` step
                //order by steps include the order on fields in the select clause which conflicts with the `count` and `group by` sql
                if (this.currentReplacedStep.getSqlgComparatorHolder().hasComparators()) {
                    return false;
                }
                int stepIndex = TraversalHelper.stepIndex(step, this.traversal);
                Step previous = this.traversal.getSteps().get(stepIndex - 1);
                if (!(previous instanceof SqlgPropertiesStep)) {
                    return handleAggregateGlobalStep(this.currentReplacedStep, step, count);
                } else {
                    return false;
                }
            } else if (step instanceof GroupStep groupStep) {
                return handleGroupStep(this.currentReplacedStep, groupStep);
            } else if (step instanceof GroupCountStep groupCountStep) {
                return handleGroupCountStep(this.currentReplacedStep, groupCountStep);
//            } else if (step instanceof FoldStep) {
//                return handleFoldStep(this.currentReplacedStep, (FoldStep)step);
            } else if (step instanceof NotStep notStep) {
                return handleNotStep(notStep, pathCount);
            } else {
                throw new IllegalStateException("Unhandled step " + step.getClass().getName());
            }
        }
        return true;
    }

    private void removeTinkerPopLabels(Step<?, ?> step) {
        Set<String> labelCopy = new HashSet<>(step.getLabels());
        for (String label : labelCopy) {
            if (Graph.Hidden.isHidden(label)) {
                step.removeLabel(label);
            }
        }
    }

    private boolean handleIdStep(Step<?, ?> step) {
        Step<?, ?> dropStep = SqlgTraversalUtil.stepAfter(this.traversal, DropStep.class, step);
        if (dropStep != null) {
            return false;
        }
        Step<?, ?> orderGlobalStep = SqlgTraversalUtil.stepAfter(this.traversal, OrderGlobalStep.class, step);
        if (orderGlobalStep != null) {
            return false;
        }
        Step<?, ?> selectOneStep = SqlgTraversalUtil.stepAfter(this.traversal, SelectOneStep.class, step);
        if (selectOneStep != null) {
            return false;
        }
        Step<?, ?> selectStep = SqlgTraversalUtil.stepAfter(this.traversal, SelectStep.class, step);
        if (selectStep != null) {
            return false;
        }
        Step<?, ?> lambdaStep = SqlgTraversalUtil.lastLambdaHolderBefore(this.traversal, step);
        if (lambdaStep == null) {
            SqlgIdStep sqlgIdStep = new SqlgIdStep(traversal);
            for (String label : step.getLabels()) {
                sqlgIdStep.addLabel(label);
            }
            //noinspection unchecked,rawtypes
            TraversalHelper.replaceStep((Step) step, sqlgIdStep, traversal);
            this.currentReplacedStep.setIdOnly(true);
        }
        return true;
    }

    private boolean handlePropertyMapStep(Step<?, ?> step) {
        Step<?, ?> dropStep = SqlgTraversalUtil.stepAfter(this.traversal, DropStep.class, step);
        if (dropStep != null) {
            return false;
        }
        Step<?, ?> orderGlobalStep = SqlgTraversalUtil.stepAfter(this.traversal, OrderGlobalStep.class, step);
        if (orderGlobalStep != null) {
            return false;
        }
        Step<?, ?> selectOneStep = SqlgTraversalUtil.stepAfter(this.traversal, SelectOneStep.class, step);
        if (selectOneStep != null) {
            return false;
        }
        Step<?, ?> selectStep = SqlgTraversalUtil.stepAfter(this.traversal, SelectStep.class, step);
        if (selectStep != null) {
            return false;
        }
        Step<?, ?> lambdaStep = SqlgTraversalUtil.lastLambdaHolderBefore(this.traversal, step);
        if (lambdaStep == null) {
            PropertyMapStep propertyMapStep = (PropertyMapStep) step;
            List<String> propertiesToRestrict = getRestrictedProperties(step);
            if (propertiesToRestrict != null) {
                this.currentReplacedStep.getRestrictedProperties().addAll(propertiesToRestrict);
                TraversalRing traversalRing;
                try {
                    Field f = propertyMapStep.getClass().getDeclaredField("traversalRing");
                    f.setAccessible(true);
                    traversalRing = (TraversalRing) f.get(propertyMapStep);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                SqlgPropertyMapStep<?, ?> sqlgPropertyMapStep = new SqlgPropertyMapStep<>(
                        traversal,
                        propertyMapStep.getIncludedTokens(),
                        propertyMapStep.getReturnType(),
                        traversalRing,
                        propertyMapStep.getPropertyKeys());

                for (String label : step.getLabels()) {
                    sqlgPropertyMapStep.addLabel(label);
                }
                //noinspection unchecked,rawtypes
                TraversalHelper.replaceStep((Step) step, sqlgPropertyMapStep, traversal);
            }
        }
        return true;
    }

    private boolean handlePropertiesStep(ReplacedStep replacedStep, Step<?, ?> step) {
        return handlePropertiesStep(replacedStep, step, this.traversal);
    }

    private boolean handlePropertiesStep(ReplacedStep replacedStep, Step<?, ?> step, Traversal.Admin<?, ?> t) {
        Step<?, ?> dropStep = SqlgTraversalUtil.stepAfter(t, DropStep.class, step);
        if (dropStep != null) {
            return false;
        }
        Step<?, ?> orderGlobalStep = SqlgTraversalUtil.stepAfter(t, OrderGlobalStep.class, step);
        if (orderGlobalStep != null) {
            return false;
        }
        Step<?, ?> lambdaStep = SqlgTraversalUtil.lastLambdaHolderBefore(t, step);
        if (lambdaStep != null) {
            return false;
        }
        List<String> propertiesToRestrict = getRestrictedProperties(step);
        if (propertiesToRestrict != null) {
            replacedStep.getRestrictedProperties().addAll(propertiesToRestrict);
        }
        PropertiesStep propertiesStep = (PropertiesStep) step;
        SqlgPropertiesStep sqlgPropertiesStep = new SqlgPropertiesStep(propertiesStep.getTraversal(), propertiesStep.getReturnType(), propertiesStep.getPropertyKeys());
        for (String label : step.getLabels()) {
            sqlgPropertiesStep.addLabel(label);
        }
        TraversalHelper.replaceStep((Step) step, sqlgPropertiesStep, t);
        return true;
    }

    private boolean handleElementMapStep(ReplacedStep replacedStep, Step<?, ?> step) {
        return handleElementMapStep(replacedStep, step, this.traversal);
    }

    private boolean handleElementMapStep(ReplacedStep replacedStep, Step<?, ?> step, Traversal.Admin<?, ?> t) {
        Step<?, ?> dropStep = SqlgTraversalUtil.stepAfter(t, DropStep.class, step);
        if (dropStep != null) {
            return false;
        }
        Step<?, ?> orderGlobalStep = SqlgTraversalUtil.stepAfter(t, OrderGlobalStep.class, step);
        if (orderGlobalStep != null) {
            return false;
        }
        Step<?, ?> lambdaStep = SqlgTraversalUtil.lastLambdaHolderBefore(t, step);
        if (lambdaStep != null) {
            return false;
        }
        List<String> propertiesToRestrict = getRestrictedProperties(step);
        if (propertiesToRestrict != null) {
            replacedStep.getRestrictedProperties().addAll(propertiesToRestrict);
        }
        ElementMapStep elementMapStep = (ElementMapStep) step;
        SqlgElementMapStep sqlgPropertiesStep = new SqlgElementMapStep(elementMapStep.getTraversal(), elementMapStep.getPropertyKeys());
        for (String label : step.getLabels()) {
            sqlgPropertiesStep.addLabel(label);
        }
        TraversalHelper.replaceStep((Step) step, sqlgPropertiesStep, t);
        return true;
    }

    private void handleDropStep() {
        this.currentReplacedStep.markAsDrop();
    }

    protected abstract boolean doFirst(ListIterator<Step<?, ?>> stepIterator, Step<?, ?> step, MutableInt pathCount);

    private void handleRecursiveRepeatStep(RepeatStep repeatStep, MutableInt pathCount, ListIterator<Step<?, ?>> stepIterator) {
        Traversal.Admin<?, ?> repeatTraversal = repeatStep.getRepeatTraversal();
        List<Step> repeatTraversalSteps = repeatTraversal.getSteps();

        //if the traversal is a topology() traversal, then remove the additional HasContainer
        List<Step> isForSqlgSchemaHasSteps = repeatTraversalSteps.stream()
                .filter(
                        s -> s instanceof HasStep<?> hasStep &&
                                hasStep.getHasContainers().get(0).getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_SQLG_SCHEMA))
                .toList();
        boolean isForSqlgSchema = !isForSqlgSchemaHasSteps.isEmpty();
        if (isForSqlgSchema) {
            for (Step isForSqlgSchemaHasStep : isForSqlgSchemaHasSteps) {
                repeatTraversal.removeStep(isForSqlgSchemaHasStep);
            }

        }
        boolean includeEdge = repeatTraversalSteps.stream().anyMatch(s -> s instanceof EdgeVertexStep || s instanceof EdgeOtherVertexStep);

        VertexStep repeatVertexStep = (VertexStep) repeatTraversalSteps.get(0);
        EdgeVertexStep repeatEdgeVertexStep;
        EdgeOtherVertexStep repeatEdgeOtherVertexStep;
        PathFilterStep repeatPathFilterStep;
        RepeatStep.RepeatEndStep repeatEndStep;
        String vertexStepLabel = "e";
        String edgeVertexStepLabel = "v";
        if (!includeEdge) {
            repeatPathFilterStep = (PathFilterStep) repeatTraversalSteps.get(1);
            repeatEndStep = (RepeatStep.RepeatEndStep) repeatTraversalSteps.get(2);
        } else {
            Set<String> labels = repeatVertexStep.getLabels();
            vertexStepLabel = labels.stream().findAny().orElse("e");
            if (repeatVertexStep.getDirection() == Direction.BOTH) {
                repeatEdgeOtherVertexStep = (EdgeOtherVertexStep) repeatTraversalSteps.get(1);
                labels = repeatEdgeOtherVertexStep.getLabels();
                edgeVertexStepLabel = labels.stream().findAny().orElse("v");
            } else {
                repeatEdgeVertexStep = (EdgeVertexStep) repeatTraversalSteps.get(1);
                labels = repeatEdgeVertexStep.getLabels();
                edgeVertexStepLabel = labels.stream().findAny().orElse("v");
            }
            repeatPathFilterStep = (PathFilterStep) repeatTraversalSteps.get(2);
            repeatEndStep = (RepeatStep.RepeatEndStep) repeatTraversalSteps.get(3);
        }

        //We create a separate SchemaTableTree for the untilTraversal.
        //i.e. create a new ReplacedStep just for the untilTraversal.
        Traversal.Admin untilTraversal = repeatStep.getUntilTraversal();
        //Make sure the untilTraversal is always wrapped in a ConnectiveStep.
        //This is because the AndOrHasContainer holds the child HasContainers in a map keyed by label.
        //This means that later when constructing the where clause the AndOrHasContainer knows how to find the
        // correct labeled SchemaTableTree to place the HasContainer on.
        List<Step<?, ?>> untilTraversalSteps = new ArrayList<>(untilTraversal.getSteps());
        if (!isForSqlgSchema) {
            List<Step<?, ?>> untilIsForSqlgSchemaHasSteps = untilTraversalSteps.stream()
                    .filter(
                            s -> s instanceof HasStep<?> hasStep &&
                                    hasStep.getHasContainers().get(0).getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_SQLG_SCHEMA))
                    .toList();
            for (Step<?, ?> isForSqlgSchemaHasStep : untilIsForSqlgSchemaHasSteps) {
                untilTraversal.removeStep(isForSqlgSchemaHasStep);
            }
        }
        List<Step<?, ?>> _untilTraversalSteps;
        Traversal.Admin _untilTraversal;
        boolean hasNotStep = false;
        boolean hasLoopAndIsStep = false;
        if (untilTraversalSteps.get(0) instanceof OrStep<?> orStep) {
            //remove the NotStep
            _untilTraversal = untilTraversal;
            List<? extends Traversal.Admin<?, ?>> connectiveTraversals = orStep.getLocalChildren();
            //Every step that is not the NotStep must have a SelectOneStep
            Traversal.Admin<?, ?> notStepTraversal = null;
            for (Traversal.Admin<?, ?> connectiveTraversal : connectiveTraversals) {
                List<NotStep> notSteps = TraversalHelper.getStepsOfAssignableClass(NotStep.class, connectiveTraversal);
                hasNotStep = hasNotStep || !notSteps.isEmpty();
                hasLoopAndIsStep = hasLoopAndIsStep ||
                        (!TraversalHelper.getStepsOfAssignableClass(LoopsStep.class, untilTraversal).isEmpty() &&
                                !TraversalHelper.getStepsOfAssignableClass(IsStep.class, untilTraversal).isEmpty());
                for (NotStep notStep : notSteps) {
                    connectiveTraversal.removeStep(notStep);
                    notStepTraversal = connectiveTraversal;
                }
                if (TraversalHelper.getStepsOfAssignableClass(SelectOneStep.class, connectiveTraversal).isEmpty()) {
                    connectiveTraversal.asAdmin().addStep(0, new SelectOneStep<>(connectiveTraversal, Pop.last, "v"));
                }
            }
            if (notStepTraversal != null) {
                orStep.getLocalChildren().remove(notStepTraversal);
            }
            _untilTraversalSteps = new ArrayList<>(_untilTraversal.getSteps());
        } else {
            //warp the untilTraversal in a AndConnectiveStep, make sure the first step after the AndStep is a SelectOneStep
            hasNotStep = !TraversalHelper.getStepsOfAssignableClassRecursively(NotStep.class, untilTraversal).isEmpty();
            hasLoopAndIsStep = !TraversalHelper.getStepsOfAssignableClass(LoopsStep.class, untilTraversal).isEmpty() &&
                    !TraversalHelper.getStepsOfAssignableClass(IsStep.class, untilTraversal).isEmpty();
            _untilTraversal = new DefaultGraphTraversal<>();
            _untilTraversal.asAdmin().addStep(new AndStep<Vertex>(_untilTraversal, untilTraversal));
            if (!(untilTraversal.getSteps().get(0) instanceof SelectOneStep<?, ?>)) {
                untilTraversal.asAdmin().addStep(0, new SelectOneStep<>(_untilTraversal, Pop.last, "v"));
            }
            _untilTraversalSteps = new ArrayList<>(_untilTraversal.getSteps());
        }

        ListIterator<Step<?, ?>> untilTraversalStepIterator = _untilTraversalSteps.listIterator();

        ReplacedStep<Vertex, Vertex> untilReplacedStep = ReplacedStep.from(
                this.sqlgGraph.getTopology(),
                (AbstractStep<Vertex, Vertex>) this.currentReplacedStep.getStep(),
                pathCount.getValue()
        );
        if (isForSqlgSchema) {
            untilReplacedStep.markForSqlgSchema();
        }
        //edgeVertexStepLabel defaults to 'v'
        untilReplacedStep.getLabels().add(edgeVertexStepLabel);
        untilReplacedStep.getLabelHasContainers().addAll(this.currentReplacedStep.getLabelHasContainers());
        ReplacedStepTree untilReplacedStepTree = new ReplacedStepTree<>(untilReplacedStep);
        this.currentReplacedStep.setRecursiveRepeatStepConfig(
                new RecursiveRepeatStepConfig(
                        repeatVertexStep.getDirection(), repeatVertexStep.getEdgeLabels()[0],
                        includeEdge,
                        untilReplacedStepTree,
                        hasNotStep,
                        hasLoopAndIsStep
                )
        );
        if (includeEdge) {
            //If the direction is BOTH we change it to OUT as the sql handles both directions at the same time
            //and we do not want both directions in the SchemaTableTree
            this.currentReplacedStep = ReplacedStep.from(
                    this.sqlgGraph.getTopology(),
                    repeatVertexStep.getDirection() == Direction.BOTH ? new VertexStep(repeatVertexStep.getTraversal(), repeatVertexStep.getReturnClass(), Direction.OUT) : repeatVertexStep,
                    pathCount.getValue()
            );
            if (isForSqlgSchema) {
                this.currentReplacedStep.markForSqlgSchema();
            }
            //Important to add the replacedStep before collecting the additional steps.
            //In particular the orderGlobalStep needs to have the currentStepDepth set.
            this.currentTreeNodeNode = this.sqlgStep.addReplacedStep(this.currentReplacedStep);

            //until traversal
            ReplacedStep<Vertex, Vertex> edgeUntilReplacedStep = ReplacedStep.from(
                    this.sqlgGraph.getTopology(),
                    repeatVertexStep.getDirection() == Direction.BOTH ? new VertexStep(repeatVertexStep.getTraversal(), repeatVertexStep.getReturnClass(), Direction.OUT) : repeatVertexStep,
                    pathCount.getValue()
            );
            if (isForSqlgSchema) {
                edgeUntilReplacedStep.markForSqlgSchema();
            }
            //clear the standard Sqlg label strategy
            edgeUntilReplacedStep.getLabels().clear();
            //label defaults to 'e'
            edgeUntilReplacedStep.getLabels().add(vertexStepLabel);
            untilReplacedStepTree.addReplacedStep(edgeUntilReplacedStep);
            edgeUntilReplacedStep.setDepth(1);

            handleConnectiveSteps(edgeUntilReplacedStep, _untilTraversal, untilTraversalStepIterator, pathCount);

        } else {
            handleHasSteps(untilReplacedStep, _untilTraversal, untilTraversalStepIterator, pathCount.getValue());
            handleConnectiveSteps(untilReplacedStep, _untilTraversal, untilTraversalStepIterator, pathCount);
            handleLoopsStep(untilReplacedStep, _untilTraversal, untilTraversalStepIterator, pathCount.getValue());
        }

        int index = TraversalHelper.stepIndex(repeatStep, this.traversal);
        if (index != -1) {
            this.traversal.removeStep(repeatStep);
        }
    }

    private void handleCallStep(ListIterator<Step<?, ?>> stepIterator, CallStep<?, ?> callStep, MutableInt pathCount) {
        String serviceName = callStep.getServiceName();

        switch (serviceName) {
            case SqlgPGRoutingFactory.NAME:
                handlePgRouting(serviceName, callStep, pathCount);
                break;
            case SqlgPGVectorFactory.NAME:
                handlePGVector(serviceName, callStep, pathCount);
                break;
            case SqlgFunctionFactory.NAME:
                handleFunction(serviceName, callStep, pathCount);
                break;
            default:
                throw new IllegalStateException("Unknown serviceName: " + serviceName);
        }


    }

    private void handlePgRouting(String serviceName, CallStep<?, ?> callStep, MutableInt pathCount) {
        Preconditions.checkArgument(serviceName.equals(SqlgPGRoutingFactory.NAME), "Only '%s' is supported, instead got '%s'", SqlgPGRoutingFactory.NAME, serviceName);
        Map param = callStep.getMergedParams();
        String function = (String) param.get(SqlgPGRoutingFactory.Params.FUNCTION);
        Preconditions.checkState(function != null, "function is null");
        Preconditions.checkState(function.equals(SqlgPGRoutingFactory.pgr_dijkstra) ||
                        function.equals(SqlgPGRoutingFactory.pgr_drivingDistance) ||
                        function.equals(SqlgPGRoutingFactory.pgr_connectedComponents),
                "Unknown callStep function %s", function);

        switch (function) {
            case SqlgPGRoutingFactory.pgr_dijkstra:
                handlePgrDijkstra(callStep, pathCount, param);
                break;
            case SqlgPGRoutingFactory.pgr_drivingDistance:
                handlePgrDrivingDistance(callStep, pathCount, param);
                break;
            case SqlgPGRoutingFactory.pgr_connectedComponents:
                handlePgrConnectedComponents(callStep, pathCount, param);
                break;
            default:
                throw new IllegalStateException("Unknown callStep function " + function);
        }

    }

    private void handleFunction(String serviceName, CallStep<?, ?> callStep, MutableInt pathCount) {
        Preconditions.checkArgument(serviceName.equals(SqlgFunctionFactory.NAME), "Only '%s' is supported, instead got '%s'", SqlgFunctionFactory.NAME, serviceName);
        Map param = callStep.getMergedParams();
        Function<Object, String> function = (Function<Object, String>) param.get(SqlgFunctionFactory.Params.FUNCTION_AS_STRING_PRODUCER);
        Preconditions.checkState(function != null, "function is null");
        this.currentReplacedStep.setSqlgFunctionConfig(
                new SqlgFunctionConfig(function)
        );
        this.traversal.removeStep(callStep);
    }

    private void handlePGVector(String serviceName, CallStep<?, ?> callStep, MutableInt pathCount) {
        Preconditions.checkArgument(serviceName.equals(SqlgPGVectorFactory.NAME), "Only '%s' is supported, instead got '%s'", SqlgPGVectorFactory.NAME, serviceName);
        Map param = callStep.getMergedParams();

        String function = (String) param.get(SqlgPGVectorFactory.Params.FUNCTION);
        Preconditions.checkState(function != null, "function is null");
        Preconditions.checkState(function.equals(SqlgPGVectorFactory.l2distance), "Unknown callStep function %s", function);

        handlePGVectorL2Distance(callStep, pathCount, param);
    }

    private void handlePGVectorL2Distance(CallStep<?, ?> callStep, MutableInt pathCount, Map param) {

        String source = (String) param.get(SqlgPGVectorFactory.Params.SOURCE);
        float[] target = (float[]) param.get(SqlgPGVectorFactory.Params.TARGET);

        GraphStep graphStep = (GraphStep) this.currentReplacedStep.getStep();
        List<HasContainer> labelHasContainers = this.currentReplacedStep.getLabelHasContainers();
        Preconditions.checkState(labelHasContainers.size() == 1);
        HasContainer labelHasContainer = labelHasContainers.get(0);
        String _abstractLabel = (String) labelHasContainer.getValue();
        SchemaTable schemaTable = SchemaTable.from(sqlgGraph, _abstractLabel);
        Optional<Schema> schemaOpt = this.sqlgGraph.getTopology().getSchema(schemaTable.getSchema());
        Preconditions.checkState(schemaOpt.isPresent());
        Schema schema = schemaOpt.get();
        AbstractLabel abstractLabel = null;
        Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(_abstractLabel);
        if (vertexLabelOptional.isPresent()) {
            abstractLabel = vertexLabelOptional.get();
        } else {
            Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(_abstractLabel);
            if (edgeLabelOptional.isPresent()) {
                abstractLabel = edgeLabelOptional.get();
            }
        }
        Preconditions.checkNotNull(abstractLabel, "Failed to find %s", _abstractLabel);
        this.currentReplacedStep.setPgVectorConfig(
                new PGVectorConfig(SqlgPGVectorFactory.l2distance, source, target, abstractLabel)
        );
        this.traversal.removeStep(callStep);
    }

    private void handlePgrDijkstra(CallStep<?, ?> callStep, MutableInt pathCount, Map param) {
        Long start_vid = (Long) param.get(SqlgPGRoutingFactory.Params.START_VID);
        List<Long> start_vids = (List<Long>) param.get(SqlgPGRoutingFactory.Params.START_VIDS);
        Long end_vid = (Long) param.get(SqlgPGRoutingFactory.Params.END_VID);
        List<Long> end_vids = (List<Long>) param.get(SqlgPGRoutingFactory.Params.END_VIDS);

        Preconditions.checkState(start_vid != null || start_vids != null, "start_vid or start_vids must be set");
        Preconditions.checkState(end_vid != null || end_vids != null, "end_vid or end_vids must be set");

        List<Long> _startVids = new ArrayList<>();
        if (start_vid != null) {
            _startVids.add(start_vid);
        } else {
            _startVids.addAll(start_vids);
        }

        List<Long> _endVids = new ArrayList<>();
        if (end_vid != null) {
            _endVids.add(end_vid);
        } else {
            _endVids.addAll(end_vids);
        }

        Boolean directed = (Boolean) param.get(SqlgPGRoutingFactory.Params.DIRECTED);

        GraphStep graphStep = (GraphStep) this.currentReplacedStep.getStep();
        Preconditions.checkState(graphStep.returnsEdge());
        List<HasContainer> labelHasContainers = this.currentReplacedStep.getLabelHasContainers();
        Preconditions.checkState(labelHasContainers.size() == 1);
        HasContainer labelHasContainer = labelHasContainers.get(0);
        String _edgeLabel = (String) labelHasContainer.getValue();
        SchemaTable schemaTable = SchemaTable.from(sqlgGraph, _edgeLabel);
        Optional<Schema> schemaOpt = this.sqlgGraph.getTopology().getSchema(schemaTable.getSchema());
        Preconditions.checkState(schemaOpt.isPresent());
        Schema schema = schemaOpt.get();
        Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(_edgeLabel);
        Preconditions.checkState(edgeLabelOptional.isPresent());
        EdgeLabel edgeLabel = edgeLabelOptional.get();
        Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();
        Set<VertexLabel> outVertexLabels = edgeLabel.getOutVertexLabels();
        Preconditions.checkState(inVertexLabels.size() == 1);
        Preconditions.checkState(outVertexLabels.size() == 1);
        Preconditions.checkState(inVertexLabels.equals(outVertexLabels));

        VertexLabel vertexLabel = inVertexLabels.iterator().next();

        this.currentReplacedStep.addLabel("a");

        this.currentReplacedStep.setPgRoutingDijkstraConfig(
                new PGRoutingDijkstraConfig(SqlgPGRoutingFactory.pgr_dijkstra, _startVids, _endVids, directed != null ? directed : false, vertexLabel, edgeLabel)
        );

        this.currentReplacedStep = ReplacedStep.from(
                this.sqlgGraph.getTopology(),
                new EdgeVertexStep(traversal, Direction.OUT),
                pathCount.getValue()
        );
        this.currentTreeNodeNode = this.sqlgStep.addReplacedStep(this.currentReplacedStep);
        this.traversal.removeStep(callStep);
        this.traversal.addStep(new PathStep<>(traversal));

    }

    private void handlePgrDrivingDistance(CallStep<?, ?> callStep, MutableInt pathCount, Map param) {
        Long start_vid = (Long) param.get(SqlgPGRoutingFactory.Params.START_VID);
        List<Long> start_vids = (List<Long>) param.get(SqlgPGRoutingFactory.Params.START_VIDS);

        Long distance = (Long) param.get(SqlgPGRoutingFactory.Params.DISTANCE);
        Preconditions.checkState(start_vid != null || start_vids != null, "start_vid or start_vids must be set");
        Preconditions.checkNotNull(distance, "distance must be set for %s", SqlgPGRoutingFactory.pgr_drivingDistance);

        List<Long> _startVids = new ArrayList<>();
        if (start_vid != null) {
            _startVids.add(start_vid);
        } else {
            _startVids.addAll(start_vids);
        }

        Boolean directed = (Boolean) param.get(SqlgPGRoutingFactory.Params.DIRECTED);

        GraphStep graphStep = (GraphStep) this.currentReplacedStep.getStep();
        Preconditions.checkState(graphStep.returnsEdge());
        List<HasContainer> labelHasContainers = this.currentReplacedStep.getLabelHasContainers();
        Preconditions.checkState(labelHasContainers.size() == 1);
        HasContainer labelHasContainer = labelHasContainers.get(0);
        String _edgeLabel = (String) labelHasContainer.getValue();
        SchemaTable schemaTable = SchemaTable.from(sqlgGraph, _edgeLabel);
        Optional<Schema> schemaOpt = this.sqlgGraph.getTopology().getSchema(schemaTable.getSchema());
        Preconditions.checkState(schemaOpt.isPresent());
        Schema schema = schemaOpt.get();
        Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(_edgeLabel);
        Preconditions.checkState(edgeLabelOptional.isPresent());
        EdgeLabel edgeLabel = edgeLabelOptional.get();
        Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();
        Set<VertexLabel> outVertexLabels = edgeLabel.getOutVertexLabels();
        Preconditions.checkState(inVertexLabels.size() == 1);
        Preconditions.checkState(outVertexLabels.size() == 1);
        Preconditions.checkState(inVertexLabels.equals(outVertexLabels));

        VertexLabel vertexLabel = inVertexLabels.iterator().next();

        this.currentReplacedStep.addLabel("a");

        this.currentReplacedStep.setPgRoutingDrivingDistanceConfig(
                new PGRoutingDrivingDistanceConfig(SqlgPGRoutingFactory.pgr_drivingDistance, _startVids, directed != null ? directed : false, distance, vertexLabel, edgeLabel)
        );

        this.currentReplacedStep = ReplacedStep.from(
                this.sqlgGraph.getTopology(),
                new EdgeVertexStep(traversal, Direction.OUT),
                pathCount.getValue()
        );
        this.currentTreeNodeNode = this.sqlgStep.addReplacedStep(this.currentReplacedStep);
        this.traversal.removeStep(callStep);
        this.traversal.addStep(new PathStep<>(traversal));

    }

    private void handlePgrConnectedComponents(CallStep<?, ?> callStep, MutableInt pathCount, Map param) {

        GraphStep graphStep = (GraphStep) this.currentReplacedStep.getStep();
        Preconditions.checkState(graphStep.returnsEdge());
        List<HasContainer> labelHasContainers = this.currentReplacedStep.getLabelHasContainers();
        Preconditions.checkState(labelHasContainers.size() == 1);
        HasContainer labelHasContainer = labelHasContainers.get(0);
        String _edgeLabel = (String) labelHasContainer.getValue();
        SchemaTable schemaTable = SchemaTable.from(sqlgGraph, _edgeLabel);
        Optional<Schema> schemaOpt = this.sqlgGraph.getTopology().getSchema(schemaTable.getSchema());
        Preconditions.checkState(schemaOpt.isPresent());
        Schema schema = schemaOpt.get();
        Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(_edgeLabel);
        Preconditions.checkState(edgeLabelOptional.isPresent());
        EdgeLabel edgeLabel = edgeLabelOptional.get();
        Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();
        Set<VertexLabel> outVertexLabels = edgeLabel.getOutVertexLabels();
        Preconditions.checkState(inVertexLabels.size() == 1);
        Preconditions.checkState(outVertexLabels.size() == 1);
        Preconditions.checkState(inVertexLabels.equals(outVertexLabels));

        VertexLabel vertexLabel = inVertexLabels.iterator().next();

//        this.currentReplacedStep.addLabel("a");

        this.currentReplacedStep.setPgRoutingConnectedComponentConfig(
                new PGRoutingConnectedComponentConfig(SqlgPGRoutingFactory.pgr_connectedComponents, vertexLabel, edgeLabel)
        );

        this.currentReplacedStep = ReplacedStep.from(
                this.sqlgGraph.getTopology(),
                new EdgeVertexStep(traversal, Direction.OUT),
                pathCount.getValue()
        );
        this.currentTreeNodeNode = this.sqlgStep.addReplacedStep(this.currentReplacedStep);
        this.traversal.removeStep(callStep);

    }


    private void handleVertexStep(ListIterator<Step<?, ?>> stepIterator, AbstractStep<?, ?> step, MutableInt pathCount, boolean notStep) {
        this.currentReplacedStep = ReplacedStep.from(
                this.sqlgGraph.getTopology(),
                step,
                pathCount.getValue()
        );
        //Important to add the replacedStep before collecting the additional steps.
        //In particular the orderGlobalStep needs to have the currentStepDepth set.
        ReplacedStepTree.TreeNode treeNodeNode = this.sqlgStep.addReplacedStep(this.currentReplacedStep);
        handleHasSteps(this.currentReplacedStep, this.traversal, stepIterator, pathCount.getValue());
        handleOrderGlobalSteps(stepIterator, pathCount);
        handleRangeGlobalSteps(stepIterator, pathCount);
        handleConnectiveSteps(this.currentReplacedStep, this.traversal, stepIterator, pathCount);
        //if called from ChooseStep then the VertexStep is nested inside the ChooseStep and not one of the traversal's direct steps.
        int index = TraversalHelper.stepIndex(step, this.traversal);
        if (index != -1) {
            this.traversal.removeStep(step);
        }
        //notSteps must not be labelled as they are going to make it to the select clause.
        if (!notStep && !this.currentReplacedStep.hasLabels()) {
            //CountGlobalStep is special, as the select statement will contain no properties
            boolean precedesPathStep = precedesPathOrTreeStep(this.traversal);
            if (precedesPathStep) {
                this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
            }
        }
        pathCount.increment();
        this.currentTreeNodeNode = treeNodeNode;
    }

    private void handleRepeatStep(RepeatStep<?> repeatStep, MutableInt pathCount) {
        List<? extends Traversal.Admin<?, ?>> repeatTraversals = repeatStep.getGlobalChildren();
        Traversal.Admin admin = repeatTraversals.get(0);
        LoopTraversal loopTraversal;
        long numberOfLoops;
        loopTraversal = (LoopTraversal) repeatStep.getUntilTraversal();
        numberOfLoops = loopTraversal.getMaxLoops();
        for (int i = 0; i < numberOfLoops; i++) {
            @SuppressWarnings("unchecked")
            ListIterator<Step<?, ?>> repeatStepIterator = admin.getSteps().listIterator();
            while (repeatStepIterator.hasNext()) {
                Step internalRepeatStep = repeatStepIterator.next();
                if (internalRepeatStep instanceof RepeatStep.RepeatEndStep) {
                    break;
                } else if (internalRepeatStep instanceof VertexStep || internalRepeatStep instanceof EdgeVertexStep || internalRepeatStep instanceof EdgeOtherVertexStep) {
                    ReplacedStep<?, ?> replacedStepToEmit;
                    //this means the ReplacedStep before the RepeatStep need to be emitted.
                    //i.e. the currentReplacedStep before running handleVertexStep needs to be emitted.
                    if (repeatStep.emitFirst) {
                        replacedStepToEmit = this.currentReplacedStep;
                        pathCount.decrement();
                        setupReplacedStepToEmit(repeatStep, pathCount, replacedStepToEmit, i == numberOfLoops - 1);
                        pathCount.increment();
                    }
                    handleVertexStep(repeatStepIterator, (AbstractStep<?, ?>) internalRepeatStep, pathCount, false);
                    pathCount.decrement();
                    if (!repeatStep.emitFirst) {
                        replacedStepToEmit = this.currentReplacedStep;
                        setupReplacedStepToEmit(repeatStep, pathCount, replacedStepToEmit, i == numberOfLoops - 1);
                    }
                    pathCount.increment();
                    //If there is an emit we can not continue the optimization.
                    this.reset = repeatStep.getEmitTraversal() != null;
                } else {
                    throw new IllegalStateException("Unhandled step nested in RepeatStep " + internalRepeatStep.getClass().getName());
                }
            }
        }
        this.traversal.removeStep(repeatStep);
    }

    private void setupReplacedStepToEmit(RepeatStep<?> repeatStep, MutableInt pathCount, ReplacedStep<?, ?> replacedStepToEmit, boolean isLast) {
        replacedStepToEmit.setEmit(repeatStep.getEmitTraversal() != null);
        replacedStepToEmit.setUntilFirst(repeatStep.untilFirst);
        if (isLast && repeatStep.getLabels().isEmpty()) {
            replacedStepToEmit.addLabel(pathCount + BaseStrategy.EMIT_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
        } else {
            for (String label : repeatStep.getLabels()) {
                replacedStepToEmit.addLabel(pathCount + BaseStrategy.EMIT_LABEL_SUFFIX + label);
            }
        }
    }

    private void handleOptionalStep(int optionalStepNestedCount, OptionalStep<?> optionalStep, Traversal.Admin<?, ?> traversal, MutableInt pathCount) {
        //The currentTreeNode here is the node that will need the left join in the sql generation
        this.optionalStepStack.add(this.currentTreeNodeNode);
        Preconditions.checkState(this.optionalStepStack.size() == optionalStepNestedCount);

        Traversal.Admin<?, ?> optionalTraversal = optionalStep.getLocalChildren().get(0);

        ReplacedStep<?, ?> previousReplacedStep = this.sqlgStep.getReplacedSteps().get(this.sqlgStep.getReplacedSteps().size() - 1);
        previousReplacedStep.setLeftJoin(true);

        @SuppressWarnings("unchecked")
        List<Step<?, ?>> optionalTraversalSteps = new ArrayList(optionalTraversal.getSteps());
        ListIterator<Step<?, ?>> optionalStepsIterator = optionalTraversalSteps.listIterator();
        while (optionalStepsIterator.hasNext()) {
            Step internalOptionalStep = optionalStepsIterator.next();
            removeTinkerPopLabels(internalOptionalStep);
            if (internalOptionalStep instanceof VertexStep || internalOptionalStep instanceof EdgeVertexStep || internalOptionalStep instanceof EdgeOtherVertexStep) {
                handleVertexStep(optionalStepsIterator, (AbstractStep<?, ?>) internalOptionalStep, pathCount, false);
                //if the chooseStepStack size is greater than the chooseStepNestedCount then it means the just executed
                //handleVertexStep is after nested chooseSteps.
                //This means that this VertexStep applies to the nested chooseSteps where the chooseStep was not chosen.
                //I.e. there were no results for the chooseSteps traversal.
                for (int i = optionalStepNestedCount; i < this.chooseStepStack.size(); i++) {
                    ReplacedStepTree.TreeNode treeNode = this.chooseStepStack.get(i);
                    this.currentReplacedStep.markAsJoinToLeftJoin();
                    treeNode.addReplacedStep(this.currentReplacedStep);
                }
            } else if (internalOptionalStep instanceof OptionalStep) {
                handleOptionalStep(optionalStepNestedCount + 1, (OptionalStep) internalOptionalStep, traversal, pathCount);
            } else if (internalOptionalStep instanceof ComputerAwareStep.EndStep) {
                break;
            } else if (internalOptionalStep instanceof HasStep) {
                handleHasSteps(this.currentReplacedStep, this.traversal, optionalStepsIterator, pathCount.getValue());
            } else {
                throw new IllegalStateException("Unhandled step nested in OptionalStep " + internalOptionalStep.getClass().getName());
            }
        }
        //the chooseStep might be a ChooseStep nested inside another ChooseStep.
        //In that case it will not be a direct step of the traversal.
        if (traversal.getSteps().contains(optionalStep)) {
            traversal.removeStep(optionalStep);
        }
    }

    private void handleChooseStep(int chooseStepNestedCount, ChooseStep<?, ?, ?> chooseStep, Traversal.Admin<?, ?> traversal, MutableInt pathCount) {
        //The currentTreeNode here is the node that will need the left join in the sql generation
        this.chooseStepStack.add(this.currentTreeNodeNode);
        Preconditions.checkState(this.chooseStepStack.size() == chooseStepNestedCount);
        List<? extends Traversal.Admin<?, ?>> globalChildren = chooseStep.getGlobalChildren();
        Preconditions.checkState(globalChildren.size() == 2, "ChooseStep's globalChildren must have size 2, one for true and one for false");

        ReplacedStep<?, ?> previousReplacedStep = this.sqlgStep.getReplacedSteps().get(this.sqlgStep.getReplacedSteps().size() - 1);
        previousReplacedStep.setLeftJoin(true);
        Traversal.Admin<?, ?> trueTraversal;
        Traversal.Admin<?, ?> a = globalChildren.get(0);
        Traversal.Admin<?, ?> b = globalChildren.get(1);
        if (a.getSteps().stream().anyMatch(s -> s instanceof IdentityStep<?>)) {
            trueTraversal = b;
        } else {
            trueTraversal = a;
        }
        @SuppressWarnings("unchecked")
        List<Step<?, ?>> trueTraversalSteps = new ArrayList(trueTraversal.getSteps());
        ListIterator<Step<?, ?>> trueTraversalStepsIterator = trueTraversalSteps.listIterator();
        while (trueTraversalStepsIterator.hasNext()) {
            Step internalChooseStep = trueTraversalStepsIterator.next();
            if (internalChooseStep instanceof VertexStep || internalChooseStep instanceof EdgeVertexStep || internalChooseStep instanceof EdgeOtherVertexStep) {
                handleVertexStep(trueTraversalStepsIterator, (AbstractStep<?, ?>) internalChooseStep, pathCount, false);
                //if the chooseStepStack size is greater than the chooseStepNestedCount then it means the just executed
                //handleVertexStep is after nested chooseSteps.
                //This means that this VertexStep applies to the nested chooseSteps where the chooseStep was not chosen.
                //I.e. there were no results for the chooseSteps traversal.
                for (int i = chooseStepNestedCount; i < this.chooseStepStack.size(); i++) {
                    ReplacedStepTree.TreeNode treeNode = this.chooseStepStack.get(i);
                    this.currentReplacedStep.markAsJoinToLeftJoin();
                    treeNode.addReplacedStep(this.currentReplacedStep);
                }
            } else if (internalChooseStep instanceof ChooseStep) {
                handleChooseStep(chooseStepNestedCount + 1, (ChooseStep) internalChooseStep, traversal, pathCount);
            } else if (internalChooseStep instanceof ComputerAwareStep.EndStep) {
                break;
            } else {
                throw new IllegalStateException("Unhandled step nested in ChooseStep " + internalChooseStep.getClass().getName());
            }
        }
        //the chooseStep might be a ChooseStep nested inside another ChooseStep.
        //In that case it will not be a direct step of the traversal.
        if (traversal.getSteps().contains(chooseStep)) {
            traversal.removeStep(chooseStep);
        }
    }

    protected abstract SqlgStep constructSqlgStep(Step startStep);

    protected abstract boolean isReplaceableStep(Class<? extends Step> stepClass);

    protected abstract void replaceStepInTraversal(Step stepToReplace, SqlgStep sqlgStep);

    static void handleLoopsStep(ReplacedStep<?, ?> replacedStep, final Traversal.Admin<?, ?> traversal, ListIterator<Step<?, ?>> iterator, int pathCount) {
        LoopsStep loopsStep = null;
        IsStep isStep = null;
        int countToGoPrevious = 0;
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            countToGoPrevious++;
            String notNullKey;
            String nullKey;
            if (currentStep instanceof LoopsStep _loopsStep) {
                loopsStep = _loopsStep;
            } else if (currentStep instanceof IsStep<?> _isStep) {
                isStep = _isStep;
            } else //noinspection StatementWithEmptyBody
                if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    for (int i = 0; i < countToGoPrevious; i++) {
                        iterator.previous();
                    }
                    break;
                }
        }
        if (loopsStep != null && isStep != null) {
            replacedStep.setLoopsStepIsStepContainer(new LoopsStepIsStepContainer(loopsStep, isStep));
        }
    }

    static void handleHasSteps(ReplacedStep<?, ?> replacedStep, final Traversal.Admin<?, ?> traversal, ListIterator<Step<?, ?>> iterator, int pathCount) {
        //Collect the hasSteps
        int countToGoPrevious = 0;
        String selectLabel = AndOrHasContainer.DEFAULT;
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            countToGoPrevious++;
            String notNullKey;
            String nullKey;
            if (currentStep instanceof HasContainerHolder hasContainerHolder) {
                List<HasContainer> hasContainers = hasContainerHolder.getHasContainers();
                List<HasContainer> toRemoveHasContainers = new ArrayList<>();
                if (isNotWithMultipleColumnValue(hasContainerHolder)) {
                    toRemoveHasContainers.addAll(isForSqlgSchema(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeHasLabel(replacedStep, hasContainers));
                    //important to do optimizeIdHas after optimizeLabelHas as it might add its labels to the previous labelHasContainers labels.
                    //i.e. for neq and without 'or' logic
                    toRemoveHasContainers.addAll(optimizeIdHas(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeHas(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeWithInOut(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeBetween(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeInside(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeOutside(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeTextContains(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeArray(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeLquery(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizePGVectorPredicate(replacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeLqueryArray(replacedStep, hasContainers));
                    if (toRemoveHasContainers.size() == hasContainers.size()) {
                        if (!currentStep.getLabels().isEmpty()) {
                            final IdentityStep identityStep = new IdentityStep<>(traversal);
                            currentStep.getLabels().forEach(label -> replacedStep.addLabel(pathCount + BaseStrategy.PATH_LABEL_SUFFIX + label));
                            //noinspection unchecked
                            TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                        }
                        if (traversal.getSteps().contains(currentStep)) {
                            traversal.removeStep(currentStep);
                        }
                        iterator.remove();
                        countToGoPrevious--;
                    }
                }
            } else if ((notNullKey = isNotNullStep(currentStep)) != null) {
                replacedStep.addHasContainer(new HasContainer(notNullKey, new P<>(Existence.NOTNULL, null)));
                if (!currentStep.getLabels().isEmpty()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    currentStep.getLabels().forEach(label -> replacedStep.addLabel(pathCount + BaseStrategy.PATH_LABEL_SUFFIX + label));
                    //noinspection unchecked
                    TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                }
                if (traversal.getSteps().contains(currentStep)) {
                    traversal.removeStep(currentStep);
                }
                iterator.remove();
                countToGoPrevious--;
            } else if ((nullKey = isNullStep(currentStep)) != null) {
                replacedStep.addHasContainer(new HasContainer(nullKey, new P<>(Existence.NULL, null)));
                if (!currentStep.getLabels().isEmpty()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    currentStep.getLabels().forEach(label -> replacedStep.addLabel(pathCount + BaseStrategy.PATH_LABEL_SUFFIX + label));
                    //noinspection unchecked
                    TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                }
                if (traversal.getSteps().contains(currentStep)) {
                    traversal.removeStep(currentStep);
                }
                iterator.remove();
                countToGoPrevious--;
            } else if (currentStep instanceof SelectOneStep<?, ?> selectOneStep) {
                selectLabel = selectOneStep.getScopeKeys().iterator().next();
            } else //noinspection StatementWithEmptyBody
                if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    for (int i = 0; i < countToGoPrevious; i++) {
                        iterator.previous();
                    }
                    break;
                }
        }
    }

    private static List<HasContainer> isForSqlgSchema(ReplacedStep<?, ?> currentReplacedStep, List<HasContainer> hasContainers) {
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getKey() != null && hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_SQLG_SCHEMA)) {
                currentReplacedStep.markForSqlgSchema();
                return Collections.singletonList(hasContainer);
            }
        }
        return Collections.emptyList();
    }

    /**
     * if this is a has(property) step, returns the property key, otherwise returns null
     *
     * @param currentStep the step
     * @return the property which should be not null
     */
    private static String isNotNullStep(Step<?, ?> currentStep) {
        if (currentStep instanceof TraversalFilterStep<?> tfs) {
            List<?> c = tfs.getLocalChildren();
            if (c.size() == 1) {
                Traversal.Admin<?, ?> a = (Traversal.Admin<?, ?>) c.iterator().next();
                Step<?, ?> s = a.getEndStep();
                if (a.getSteps().size() == 1 && s instanceof PropertiesStep<?> ps) {
                    String[] keys = ps.getPropertyKeys();
                    if (keys != null && keys.length == 1) {
                        return keys[0];
                    }
                }
            }
        }
        return null;
    }

    /**
     * if this is a hasNot(property) step, returns the property key, otherwise returns null
     *
     * @param currentStep the step
     * @return the property which should be not null
     */
    private static String isNullStep(Step<?, ?> currentStep) {
        if (currentStep instanceof NotStep<?> tfs) {
            List<?> c = tfs.getLocalChildren();
            if (c.size() == 1) {
                Traversal.Admin<?, ?> a = (Traversal.Admin<?, ?>) c.iterator().next();
                Step<?, ?> s = a.getEndStep();
                if (a.getSteps().size() == 1 && s instanceof PropertiesStep<?> ps) {
                    String[] keys = ps.getPropertyKeys();
                    if (keys != null && keys.length == 1) {
                        return keys[0];
                    }
                }
            }
        }
        return null;
    }

    void handleOrderGlobalSteps(ListIterator<Step<?, ?>> iterator, MutableInt pathCount) {
        //Collect the OrderGlobalSteps
        while (iterator.hasNext()) {
            Step<?, ?> step = iterator.next();
            if (step instanceof OrderGlobalStep) {
                if (optimizableOrderGlobalStep((OrderGlobalStep) step)) {
                    //add the label if any
                    for (String label : step.getLabels()) {
                        this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + label);
                    }
                    //The step might not be here. For instance if it was nested in a chooseStep where the chooseStep logic already removed the step.
                    if (this.traversal.getSteps().contains(step)) {
                        this.traversal.removeStep(step);
                    }
                    iterator.previous();
                    Step previousStep = iterator.previous();
                    if (previousStep instanceof SelectOneStep selectOneStep) {
                        String key = (String) selectOneStep.getScopeKeys().iterator().next();
                        this.currentReplacedStep.getSqlgComparatorHolder().setPrecedingSelectOneLabel(key);
                        @SuppressWarnings("unchecked")
                        List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators = ((OrderGlobalStep) step).getComparators();
                        //get the step for the label
                        Optional<ReplacedStep<?, ?>> labeledReplacedStep = this.sqlgStep.getReplacedSteps().stream().filter(
                                r -> {
                                    //Take the first
                                    if (r.hasLabels()) {
                                        String label = r.getLabels().iterator().next();
                                        String stepLabel = SqlgUtil.originalLabel(label);
                                        return stepLabel.equals(key);
                                    } else {
                                        return false;
                                    }
                                }
                        ).findAny();
                        Preconditions.checkState(labeledReplacedStep.isPresent());
                        ReplacedStep<?, ?> replacedStep = labeledReplacedStep.get();
                        replacedStep.getSqlgComparatorHolder().setComparators(comparators);
                        //add a label if the step does not yet have one and is not a leaf node
                        if (!replacedStep.hasLabels()) {
                            replacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL);
                        }
                    } else if (previousStep instanceof OptionalStep) {
                        throw new RuntimeException("not yet implemented");
                    } else if (previousStep instanceof ChooseStep) {
                        //The order applies to the current replaced step and the previous ChooseStep
                        @SuppressWarnings("unchecked")
                        List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators = ((OrderGlobalStep) step).getComparators();
                        this.currentReplacedStep.getSqlgComparatorHolder().setComparators(comparators);
                        //add a label if the step does not yet have one and is not a leaf node
                        if (!this.currentReplacedStep.hasLabels()) {
                            this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL);
                        }
                    } else {
                        List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators = ((OrderGlobalStep) step).getComparators();
                        this.currentReplacedStep.getSqlgComparatorHolder().setComparators(comparators);
                        //add a label if the step does not yet have one and is not a leaf node
                        if (!this.currentReplacedStep.hasLabels()) {
                            this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL);
                        }
                    }
                    iterator.next();
                    iterator.next();
                } else {
                    return;
                }
            } else {
                //break on the first step that is not a OrderGlobalStep
                iterator.previous();
                break;
            }
        }
    }

    static void handleConnectiveSteps(ReplacedStep<?, ?> replacedStep, final Traversal.Admin<?, ?> traversal, ListIterator<Step<?, ?>> iterator, MutableInt pathCount) {
        //Collect the hasSteps
        int countToGoPrevious = 0;
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            countToGoPrevious++;
            if (currentStep instanceof ConnectiveStep) {
                Optional<AndOrHasContainer> outerAndOrHasContainer = handleConnectiveStepInternal((ConnectiveStep) currentStep);
                if (outerAndOrHasContainer.isPresent()) {
                    replacedStep.addAndOrHasContainer(outerAndOrHasContainer.get());
                    for (String label : currentStep.getLabels()) {
                        replacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + label);
                    }
                    traversal.removeStep(currentStep);
                    iterator.remove();
                    countToGoPrevious--;
                }
            } else //noinspection StatementWithEmptyBody
                if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    for (int i = 0; i < countToGoPrevious; i++) {
                        iterator.previous();
                    }
                    break;
                }
        }
    }

    private static Optional<AndOrHasContainer> handleConnectiveStepInternal(ConnectiveStep connectiveStep) {
        AndOrHasContainer.TYPE type = AndOrHasContainer.TYPE.from(connectiveStep);
        AndOrHasContainer outerAndOrHasContainer = new AndOrHasContainer(type);
        @SuppressWarnings("unchecked")
        List<Traversal.Admin<?, ?>> localTraversals = connectiveStep.getLocalChildren();
        for (Traversal.Admin<?, ?> localTraversal : localTraversals) {
            if (!TraversalHelper.hasAllStepsOfClass(localTraversal, HasStep.class, ConnectiveStep.class, TraversalFilterStep.class, NotStep.class, SelectOneStep.class, LoopsStep.class, IsStep.class)) {
                return Optional.empty();
            }
            AndOrHasContainer andOrHasContainer = new AndOrHasContainer(AndOrHasContainer.TYPE.NONE);
            outerAndOrHasContainer.addAndOrHasContainer(andOrHasContainer);
            LoopsStep loopsStep = null;
            IsStep isStep = null;
            String selectLabel = AndOrHasContainer.DEFAULT;
            for (Step<?, ?> step : localTraversal.getSteps()) {
                if (step instanceof HasStep) {
                    HasStep<?> hasStep = (HasStep) step;
                    for (HasContainer hasContainer : hasStep.getHasContainers()) {
                        boolean hasContainerKeyNotIdOrLabel = hasContainerKeyNotIdOrLabel(hasContainer);
                        if (hasContainerKeyNotIdOrLabel && SUPPORTED_BI_PREDICATE.contains(hasContainer.getBiPredicate())) {
                            andOrHasContainer.addHasContainer(selectLabel, hasContainer);
                        } else if (hasContainerKeyNotIdOrLabel && (hasContainer.getBiPredicate().equals(within) || hasContainer.getBiPredicate().equals(without))) {
                            andOrHasContainer.addHasContainer(selectLabel, hasContainer);
                        } else if (hasContainerKeyNotIdOrLabel && hasContainer.getPredicate() instanceof AndP) {
                            AndP<?> andP = (AndP) hasContainer.getPredicate();
                            List<? extends P<?>> predicates = andP.getPredicates();
                            if (predicates.size() == 2) {
                                if (predicates.get(0).getBiPredicate() == gte && predicates.get(1).getBiPredicate() == lt) {
                                    andOrHasContainer.addHasContainer(selectLabel, hasContainer);
                                } else if (predicates.get(0).getBiPredicate() == gt && predicates.get(1).getBiPredicate() == lt) {
                                    andOrHasContainer.addHasContainer(selectLabel, hasContainer);
                                }
                            }
                        } else if (hasContainerKeyNotIdOrLabel && hasContainer.getPredicate() instanceof OrP) {
                            OrP<?> orP = (OrP) hasContainer.getPredicate();
                            List<? extends P<?>> predicates = orP.getPredicates();
                            if (predicates.size() == 2) {
                                if (predicates.get(0).getBiPredicate() == lt && predicates.get(1).getBiPredicate() == gt) {
                                    andOrHasContainer.addHasContainer(selectLabel, hasContainer);
                                }
                            }
                        } else if (hasContainerKeyNotIdOrLabel && hasContainer.getBiPredicate() instanceof Text
                                || hasContainer.getBiPredicate() instanceof FullText
                                || hasContainer.getBiPredicate() instanceof ArrayContains
                                || hasContainer.getBiPredicate() instanceof ArrayOverlaps
                                || hasContainer.getBiPredicate() instanceof Lquery
                                || hasContainer.getBiPredicate() instanceof LqueryArray) {
                            andOrHasContainer.addHasContainer(selectLabel, hasContainer);
                        } else {
                            return Optional.empty();
                        }
                    }
                } else if (step instanceof TraversalFilterStep) {
                    String notNullKey = isNotNullStep(step);
                    if (notNullKey != null) {
                        andOrHasContainer.addHasContainer(selectLabel, new HasContainer(notNullKey, new P<>(Existence.NOTNULL, null)));
                    } else {
                        return Optional.empty();
                    }
                } else if (step instanceof NotStep) {
                    String nullKey = isNullStep(step);
                    if (nullKey != null) {
                        andOrHasContainer.addHasContainer(selectLabel, new HasContainer(nullKey, new P<>(Existence.NULL, null)));
                    } else {
                        return Optional.empty();
                    }
                } else if (step instanceof SelectOneStep<?, ?> selectOneStep) {
                    selectLabel = selectOneStep.getScopeKeys().iterator().next();
                } else if (step instanceof LoopsStep<?> _loopsStep) {
                    loopsStep = _loopsStep;
                } else if (step instanceof IsStep<?> _isStep) {
                    isStep = _isStep;
                } else {
                    ConnectiveStep connectiveStepLocalChild = (ConnectiveStep) step;
                    Optional<AndOrHasContainer> result = handleConnectiveStepInternal(connectiveStepLocalChild);
                    if (result.isPresent()) {
                        andOrHasContainer.addAndOrHasContainer(result.get());
                    } else {
                        return Optional.empty();
                    }
                }
                if (loopsStep != null && isStep != null) {
                    andOrHasContainer.setLoopsStepIsStepContainer(new LoopsStepIsStepContainer(loopsStep, isStep));
                }
            }
        }
        if (outerAndOrHasContainer.hasHasContainers()) {
            return Optional.of(outerAndOrHasContainer);
        } else {
            return Optional.empty();
        }
    }

    private boolean optimizableOrderGlobalStep(OrderGlobalStep step) {
        @SuppressWarnings("unchecked")
        List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators = step.getComparators();
        for (Pair<Traversal.Admin<?, ?>, Comparator<?>> comparator : comparators) {
            Traversal.Admin<?, ?> defaultGraphTraversal = comparator.getValue0();
            List<CountGlobalStep> countGlobalSteps = TraversalHelper.getStepsOfAssignableClassRecursively(CountGlobalStep.class, defaultGraphTraversal);
            if (!countGlobalSteps.isEmpty()) {
                return false;
            }
            List<LambdaMapStep> lambdaMapSteps = TraversalHelper.getStepsOfAssignableClassRecursively(LambdaMapStep.class, defaultGraphTraversal);
            if (!lambdaMapSteps.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    void handleRangeGlobalSteps(ListIterator<Step<?, ?>> iterator, MutableInt pathCount) {
        handleRangeGlobalSteps(null, iterator, pathCount);
    }

    void handleRangeGlobalSteps(Step fromStep, ListIterator<Step<?, ?>> iterator, MutableInt pathCount) {
        //Collect the OrderGlobalSteps
        //noinspection LoopStatementThatDoesntLoop
        while (iterator.hasNext()) {
            Step<?, ?> step = iterator.next();
            if (step instanceof RangeGlobalStep<?> rgs) {
                //add the label if any
                if (fromStep instanceof SelectStep || fromStep instanceof SelectOneStep) {
                    //this is for blah().select("x").limit(1).as("y")
                    //in this case the "y" label should go to the select step
                    for (String label : step.getLabels()) {
                        fromStep.addLabel(label);
                    }
                } else {
                    for (String label : step.getLabels()) {
                        this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + label);
                    }
                }
                //The step might not be here. For instance if it was nested in a chooseStep where the chooseStep logic already removed the step.
                if (this.traversal.getSteps().contains(step)) {
                    this.traversal.removeStep(step);
                }
                long high = rgs.getHighRange();
                if (high == -1) {
                    //skip step
                    this.currentReplacedStep.setSqlgRangeHolder(SqlgRangeHolder.from(rgs.getLowRange()));
                } else {
                    this.currentReplacedStep.setSqlgRangeHolder(SqlgRangeHolder.from(Range.of(rgs.getLowRange(), high)));
                }
                //add a label if the step does not yet have one and is not a leaf node
                if (!this.currentReplacedStep.hasLabels()) {
                    this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL);
                }
                this.reset = true;
                break;
            } else {
                //break on the first step that is not a RangeGlobalStep
                iterator.previous();
                break;
            }
        }
    }

    static boolean precedesPathOrTreeStep(Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() != null && (traversal.getParent() instanceof SqlgLocalStepBarrier || traversal.getParent() instanceof SqlgUnionStepBarrier)) {
            SqlgAbstractStep sqlgAbstractStep = (SqlgAbstractStep) traversal.getParent();
            if (precedesPathOrTreeStep(sqlgAbstractStep.getTraversal())) {
                return true;
            }
        }
        Predicate<Step> p = s -> s.getClass().equals(PathStep.class) ||
                s.getClass().equals(TreeStep.class) ||
                s.getClass().equals(TreeSideEffectStep.class) ||
                s.getClass().equals(PathFilterStep.class) ||
                s.getClass().equals(EdgeOtherVertexStep.class);
        return SqlgTraversalUtil.anyStepRecursively(p, traversal);
    }

    void addHasContainerForIds(SqlgGraphStep sqlgGraphStep, Object[] ids) {
        HasContainer idHasContainer = new HasContainer(T.id.getAccessor(), P.within(ids));
        this.currentReplacedStep.addIdHasContainer(idHasContainer);
        sqlgGraphStep.clearIds();
    }

    private static boolean isNotWithMultipleColumnValue(HasContainerHolder currentStep) {
        for (HasContainer h : currentStep.getHasContainers()) {
            P<?> predicate = h.getPredicate();
            //noinspection unchecked
            if (predicate.getValue() instanceof ZonedDateTime ||
                    predicate.getValue() instanceof Period ||
                    predicate.getValue() instanceof Duration ||
                    (predicate.getValue() instanceof List && containsWithMultipleColumnValue((List<Object>) predicate.getValue())) ||
                    (predicate instanceof ConnectiveP && isConnectivePWithMultipleColumnValue((ConnectiveP) h.getPredicate()))) {


                return false;
            }

        }
        return true;
    }

    private static boolean hasContainerKeyNotIdOrLabel(HasContainer hasContainer) {
        return hasContainer.getKey() != null &&
                !(
                        hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_SQLG_SCHEMA) ||
                                hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_GLOBAL_UNIQUE_INDEX) ||
                                hasContainer.getKey().equals(T.id.getAccessor()) ||
                                hasContainer.getKey().equals(T.label.getAccessor())
                );
    }

    private static List<HasContainer> optimizeIdHas(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getKey() != null &&
//                    hasContainer.getValue() != null &&
                    hasContainer.getKey().equals(T.id.getAccessor()) &&
                    SUPPORTED_ID_BI_PREDICATE.contains(hasContainer.getBiPredicate())) {

//                eq, neq, within, without
                if (hasContainer.getValue() == null) {
                    BiPredicate<?, ?> bp = hasContainer.getBiPredicate();
                    P<?> transformedBiPredicate;
                    if (bp == eq) {
                        transformedBiPredicate = P.eq(RecordId.fake());
                    } else if (bp == neq) {
                        transformedBiPredicate = P.neq(RecordId.fake());
                    } else if (bp == within) {
                        transformedBiPredicate = P.within(RecordId.fake());
                    } else if (bp == without) {
                        transformedBiPredicate = P.without(RecordId.fake());
                    } else {
                        throw new UnsupportedOperationException(String.format("Bipredicate %s is not supported", bp));
                    }
                    replacedStep.addIdHasContainer(
                            new HasContainer(
                                    hasContainer.getKey(),
                                    transformedBiPredicate
                            )
                    );
                } else {
                    replacedStep.addIdHasContainer(hasContainer);

                }
                result.add(hasContainer);
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeHasLabel(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getKey() != null &&
                    hasContainer.getKey().equals(T.label.getAccessor()) &&
                    SUPPORTED_LABEL_BI_PREDICATE.contains(hasContainer.getBiPredicate())) {

                replacedStep.addLabelHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeHas(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && SUPPORTED_BI_PREDICATE.contains(hasContainer.getBiPredicate())) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeWithInOut(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && (hasContainer.getBiPredicate() == without || hasContainer.getBiPredicate() == within)) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }


    private static List<HasContainer> optimizeBetween(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && hasContainer.getPredicate() instanceof AndP) {
                AndP<?> andP = (AndP) hasContainer.getPredicate();
                List<? extends P<?>> predicates = andP.getPredicates();
                if (predicates.size() == 2) {
                    if (predicates.get(0).getBiPredicate() == gte && predicates.get(1).getBiPredicate() == lt) {
                        replacedStep.addHasContainer(hasContainer);
                        result.add(hasContainer);
                    }
                }
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeInside(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && hasContainer.getPredicate() instanceof AndP) {
                AndP<?> andP = (AndP) hasContainer.getPredicate();
                List<? extends P<?>> predicates = andP.getPredicates();
                if (predicates.size() == 2) {
                    if (predicates.get(0).getBiPredicate() == gt && predicates.get(1).getBiPredicate() == lt) {
                        replacedStep.addHasContainer(hasContainer);
                        result.add(hasContainer);
                    }
                }
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeOutside(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && hasContainer.getPredicate() instanceof OrP) {
                OrP<?> orP = (OrP) hasContainer.getPredicate();
                List<? extends P<?>> predicates = orP.getPredicates();
                if (predicates.size() == 2) {
                    if (predicates.get(0).getBiPredicate() == lt && predicates.get(1).getBiPredicate() == gt) {
                        replacedStep.addHasContainer(hasContainer);
                        result.add(hasContainer);
                    }
                }
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeTextContains(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && hasContainer.getBiPredicate() instanceof Text ||
                    hasContainer.getBiPredicate() instanceof FullText
            ) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private static List<HasContainer> optimizePGVectorPredicate(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && hasContainer.getBiPredicate() instanceof PGVectorPredicate) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeLquery(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && hasContainer.getBiPredicate() instanceof Lquery) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeLqueryArray(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer) && hasContainer.getBiPredicate() instanceof LqueryArray) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private static List<HasContainer> optimizeArray(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainerKeyNotIdOrLabel(hasContainer)
                    && (hasContainer.getBiPredicate() instanceof ArrayContains
                    || hasContainer.getBiPredicate() instanceof ArrayOverlaps)) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private static boolean containsWithMultipleColumnValue(List<Object> values) {
        for (Object value : values) {
            if (value instanceof ZonedDateTime ||
                    value instanceof Period ||
                    value instanceof Duration) {
                return true;
            }
        }
        return false;
    }

    private static boolean isConnectivePWithMultipleColumnValue(ConnectiveP connectiveP) {
        @SuppressWarnings("unchecked")
        List<P<?>> ps = connectiveP.getPredicates();
        for (P<?> predicate : ps) {
            if (predicate.getValue() instanceof ZonedDateTime ||
                    predicate.getValue() instanceof Period ||
                    predicate.getValue() instanceof Duration) {
                return true;
            }
        }
        return false;
    }

    boolean canNotBeOptimized() {
        @SuppressWarnings("unchecked") final List<Step<?, ?>> steps = new ArrayList(this.traversal.asAdmin().getSteps());
        final ListIterator<Step<?, ?>> stepIterator = steps.listIterator();
        List<Step<?, ?>> toCome = steps.subList(stepIterator.nextIndex(), steps.size());
        return toCome.stream().anyMatch(s ->
                s.getClass().equals(LambdaCollectingBarrierStep.class) || s.getClass().equals(SackValueStep.class)
        );
    }

    boolean unoptimizableOptionalStep(OptionalStep<?> optionalStep) {
        if (!this.optionalStepStack.isEmpty()) {
            return true;
        }

        Traversal.Admin<?, ?> optionalTraversal = optionalStep.getLocalChildren().get(0);
        List<Step> optionalTraversalSteps = new ArrayList<>(optionalTraversal.getSteps());

        //Can not optimize if the traversal contains a RangeGlobalStep
        List<Step> rangeGlobalSteps = optionalTraversalSteps.stream().filter(p -> p.getClass().equals(RangeGlobalStep.class)).toList();
        if (rangeGlobalSteps.size() > 1) {
            return true;
        }
        if (!rangeGlobalSteps.isEmpty()) {
            Step rangeGlobalStep = rangeGlobalSteps.get(0);
            //Only if the rangeGlobalStep is the last step can it be optimized
            if (optionalTraversalSteps.get(optionalTraversalSteps.size() - 1) != rangeGlobalStep) {
                return true;
            }
        }

        for (Step internalOptionalStep : optionalTraversalSteps) {
            if (!(internalOptionalStep instanceof VertexStep || internalOptionalStep instanceof EdgeVertexStep ||
                    internalOptionalStep instanceof EdgeOtherVertexStep || internalOptionalStep instanceof ComputerAwareStep.EndStep ||
                    internalOptionalStep instanceof OptionalStep || internalOptionalStep instanceof HasStep ||
                    internalOptionalStep instanceof OrderGlobalStep || internalOptionalStep instanceof RangeGlobalStep)) {
                return true;
            }
        }

        List<Step> optionalSteps = optionalTraversalSteps.stream().filter(p -> p.getClass().equals(OptionalStep.class)).toList();
        for (Step step : optionalSteps) {
            if (unoptimizableOptionalStep((OptionalStep<?>) step)) {
                return true;
            }
        }
        return false;
    }

    boolean unoptimizableChooseStep(ChooseStep<?, ?, ?> chooseStep) {
        if (!this.chooseStepStack.isEmpty()) {
            return true;
        }
        List<? extends Traversal.Admin<?, ?>> traversalAdmins = chooseStep.getGlobalChildren();
        if (traversalAdmins.size() != 2) {
            return true;
        }
        Traversal.Admin<?, ?> predicate = chooseStep.getLocalChildren().get(0);
        List<Step> predicateSteps = new ArrayList<>(predicate.getSteps());
        if (!(predicate.getSteps().get(predicate.getSteps().size() - 1) instanceof HasNextStep)) {
            return true;
        }
        //Remove the HasNextStep
        predicateSteps.remove(predicate.getSteps().size() - 1);

        Traversal.Admin<?, ?> globalChildOne = chooseStep.getGlobalChildren().get(0);
        List<Step> globalChildOneSteps = new ArrayList<>(globalChildOne.getSteps());
        globalChildOneSteps.remove(globalChildOneSteps.size() - 1);

        Traversal.Admin<?, ?> globalChildTwo = chooseStep.getGlobalChildren().get(1);
        List<Step> globalChildTwoSteps = new ArrayList<>(globalChildTwo.getSteps());
        globalChildTwoSteps.remove(globalChildTwoSteps.size() - 1);

        boolean hasIdentity = globalChildOne.getSteps().stream().anyMatch(s -> s instanceof IdentityStep);
        if (!hasIdentity) {
            hasIdentity = globalChildTwo.getSteps().stream().anyMatch(s -> s instanceof IdentityStep);
            if (hasIdentity) {
                //Identity found check predicate and true are the same
                if (!predicateSteps.equals(globalChildOneSteps)) {
                    return true;
                }
            } else {
                //Identity not found
                return true;
            }
        } else {
            //Identity found check predicate and true are the same
            if (!predicateSteps.equals(globalChildTwoSteps)) {
                return true;
            }
        }
        List<Step> localSteps = predicateSteps.stream().filter(p -> p.getClass().equals(LocalStep.class)).toList();
        if (!localSteps.isEmpty()) {
            return true;
        }

        List<Step> rangeGlobalSteps = predicateSteps.stream().filter(p -> p.getClass().equals(RangeGlobalStep.class)).toList();
        if (rangeGlobalSteps.size() > 1) {
            return true;
        }
        if (!rangeGlobalSteps.isEmpty()) {
            Step rangeGlobalStep = rangeGlobalSteps.get(0);
            //Only if the rangeGlobalStep is the last step can it be optimized
            if (predicateSteps.get(predicateSteps.size() - 1) != rangeGlobalStep) {
                return true;
            }
        }

        Traversal.Admin<?, ?> trueTraversal;
        if (globalChildOne.getSteps().stream().anyMatch(s -> s instanceof IdentityStep<?>)) {
            trueTraversal = globalChildTwo;
        } else {
            trueTraversal = globalChildOne;
        }
        @SuppressWarnings("unchecked")
        List<Step<?, ?>> trueTraversalSteps = new ArrayList(trueTraversal.getSteps());
        for (Step<?, ?> internalChooseStep : trueTraversalSteps) {
            if (!(internalChooseStep instanceof VertexStep || internalChooseStep instanceof EdgeVertexStep ||
                    internalChooseStep instanceof EdgeOtherVertexStep || internalChooseStep instanceof ComputerAwareStep.EndStep ||
                    internalChooseStep instanceof ChooseStep || internalChooseStep instanceof HasStep ||
                    internalChooseStep instanceof OrderGlobalStep || internalChooseStep instanceof RangeGlobalStep)) {
                return true;
            }
        }

        List<Step> chooseSteps = globalChildTwo.getSteps().stream().filter(p -> p.getClass().equals(ChooseStep.class)).toList();
        for (Step step : chooseSteps) {
            if (unoptimizableChooseStep((ChooseStep<?, ?, ?>) step)) {
                return true;
            }
        }
        return false;
    }

    private boolean isRecursiveRepeatStep(RepeatStep repeatStep) {
        Traversal.Admin<?, ?> emitTraversal = repeatStep.getEmitTraversal();
        if (emitTraversal == null) {
            Traversal.Admin<?, ?> repeatTraversal = repeatStep.getRepeatTraversal();
            List<Step> repeatTraversalSteps = repeatTraversal.getSteps();
            if (
                    (
                            repeatTraversalSteps.size() == 3 &&
                                    repeatTraversalSteps.get(0) instanceof VertexStep<?> &&
                                    repeatTraversalSteps.get(1) instanceof PathFilterStep<?> pathFilterStep && pathFilterStep.isSimple() &&
                                    repeatTraversalSteps.get(2) instanceof RepeatStep.RepeatEndStep<?>
                    ) ||
                            (
                                    repeatTraversalSteps.size() == 4 &&
                                            repeatTraversalSteps.get(0) instanceof VertexStep<?> &&
                                            (repeatTraversalSteps.get(1) instanceof EdgeVertexStep || repeatTraversalSteps.get(1) instanceof EdgeOtherVertexStep) &&
                                            repeatTraversalSteps.get(2) instanceof PathFilterStep<?> _pathFilterStep && _pathFilterStep.isSimple() &&
                                            repeatTraversalSteps.get(3) instanceof RepeatStep.RepeatEndStep<?>
                            )
                            ||
                            (
                                    repeatTraversalSteps.size() == 4 &&
                                            repeatTraversalSteps.get(0) instanceof VertexStep<?> &&
                                            (repeatTraversalSteps.get(1) instanceof HasStep<?> hasStep) && hasStep.getHasContainers().get(0).getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_SQLG_SCHEMA) &&
                                            repeatTraversalSteps.get(2) instanceof PathFilterStep<?> __pathFilterStep && __pathFilterStep.isSimple() &&
                                            repeatTraversalSteps.get(3) instanceof RepeatStep.RepeatEndStep<?>
                            )
            ) {

                VertexStep<?> vertexStep = (VertexStep<?>) repeatTraversalSteps.get(0);
                String[] edgeLabels = vertexStep.getEdgeLabels();
                List<HasContainer> labelHasContainers = this.currentReplacedStep.getLabelHasContainers();
                if (edgeLabels.length == 1 && labelHasContainers.size() == 1) {
                    String _edgeLabel = edgeLabels[0];
                    HasContainer labelHasContainer = labelHasContainers.get(0);
                    String vertexLabel;
                    if (labelHasContainer.getValue() instanceof String _vertexLabel) {
                        vertexLabel = _vertexLabel;
                    } else if (labelHasContainer.getValue() instanceof Set vertexLabelSet) {
                        vertexLabel = (String) vertexLabelSet.iterator().next();
                    } else {
                        throw new IllegalStateException("Unhandled label HasContainer");
                    }
                    SchemaTable schemaTable = SchemaTable.from(sqlgGraph, vertexLabel);
                    Optional<Schema> schemaOpt = this.sqlgGraph.getTopology().getSchema(schemaTable.getSchema());
                    if (schemaOpt.isPresent()) {
                        Optional<VertexLabel> vertexLabelOptional = schemaOpt.get().getVertexLabel(schemaTable.getTable());
                        if (vertexLabelOptional.isPresent()) {
                            Optional<EdgeLabel> edgeLabelOpt = schemaOpt.get().getEdgeLabel(_edgeLabel);
                            if (edgeLabelOpt.isPresent()) {
                                EdgeLabel recursiveEdgeLabel = edgeLabelOpt.get();
                                Set<VertexLabel> inVertexLabels = recursiveEdgeLabel.getInVertexLabels();
                                Set<VertexLabel> outVertexLabels = recursiveEdgeLabel.getOutVertexLabels();
                                if (inVertexLabels.size() == 1 && outVertexLabels.size() == 1) {
                                    VertexLabel inVertexLabel = inVertexLabels.iterator().next();
                                    VertexLabel outVertexLabel = outVertexLabels.iterator().next();

                                    Preconditions.checkState(inVertexLabel.equals(vertexLabelOptional.get()));
                                    Preconditions.checkState(outVertexLabel.equals(vertexLabelOptional.get()));

                                    Traversal.Admin<?, ?> untilTraversal = repeatStep.getUntilTraversal();
                                    List<Step> untilTraversalSteps = untilTraversal.getSteps();
                                    if (untilTraversalSteps.size() == 1 && untilTraversalSteps.get(0) instanceof NotStep<?> notStep) {
                                        List<? extends Traversal.Admin<?, ?>> notStepTraversals = notStep.getLocalChildren();
                                        if (notStepTraversals.size() == 1) {
                                            Traversal.Admin notStepTraversal = notStepTraversals.get(0);
                                            List<Step> notSteps = notStepTraversal.getSteps();
                                            if (notSteps.size() == 2 &&
                                                    notSteps.get(0) instanceof VertexStep<?> notVertexStep &&
                                                    notSteps.get(1) instanceof PathFilterStep<?> notPathFilterStep) {

                                                String[] notEdgeLabels = notVertexStep.getEdgeLabels();
                                                if (notEdgeLabels.length == 1 && notEdgeLabels[0].equals(_edgeLabel)) {
                                                    return true;
                                                }
                                            } else if (notSteps.size() == 3 &&
                                                    notSteps.get(0) instanceof VertexStep<?> notVertexStep &&
                                                    notSteps.get(1) instanceof HasStep<?> hasStep && hasStep.getHasContainers().get(0).getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_SQLG_SCHEMA) &&
                                                    notSteps.get(2) instanceof PathFilterStep<?> notPathFilterStep) {

                                                String[] notEdgeLabels = notVertexStep.getEdgeLabels();
                                                if (notEdgeLabels.length == 1 && notEdgeLabels[0].equals(_edgeLabel)) {
                                                    return true;
                                                }
                                            }
                                        }
                                    } else if (untilTraversalSteps.size() == 1 && untilTraversalSteps.get(0) instanceof OrStep<?> orStep) {
                                        //check that one of the OrStep traversals is a NotStep
                                        return true;
                                    } else if (untilTraversalSteps.size() == 1 && untilTraversalSteps.get(0) instanceof AndStep<?> andStep) {
                                        //we can not optimize NotStep nested inside a top level AndStep
                                        List<? extends Traversal.Admin<?, ?>> andStepTraversals = andStep.getLocalChildren();
                                        for (Traversal.Admin<?, ?> andStepTraveral : andStepTraversals) {
                                            if (!TraversalHelper.getStepsOfAssignableClassRecursively(andStepTraveral, NotStep.class).isEmpty()) {
                                                LOGGER.debug("Unable to optimize recursive repeat step because of NotStep inside a AndStep: {}", andStepTraveral);
                                                return false;
                                            }
                                        }
                                        return true;
                                    } else if (untilTraversalSteps.size() == 1 && untilTraversalSteps.get(0) instanceof HasStep<?> hasStep) {
                                        return true;
                                    } else if (untilTraversalSteps.size() == 2 && untilTraversalSteps.get(0) instanceof LoopsStep<?> loopsStep && untilTraversalSteps.get(1) instanceof IsStep<?> isStep) {
                                        return true;
//                                    } else if (untilTraversalSteps.size() == 2 && untilTraversalSteps.get(0) instanceof LoopsStep<?> loopsStep && untilTraversalSteps.get(1) instanceof IsStep<?> isStep) {
//                                        return true;
                                    } else if (!untilTraversalSteps.isEmpty() && untilTraversalSteps.get(0) instanceof SelectOneStep<?, ?> selectOneStep) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean optimizableRepeatStep(List<RepeatStep> repeatSteps) {
        boolean hasUntil = repeatSteps.stream().allMatch(repeatStep -> repeatStep.getUntilTraversal() != null);
        boolean hasUnoptimizableUntil = false;
        if (hasUntil) {
            hasUnoptimizableUntil = repeatSteps.stream().noneMatch(repeatStep -> repeatStep.getUntilTraversal() instanceof LoopTraversal);
        }
        boolean badRepeat = !hasUntil || hasUnoptimizableUntil;
        //Check if the repeat step only contains optimizable steps
        if (!badRepeat) {
            List<Step> collectedRepeatInternalSteps = new ArrayList<>();
            for (Step step : repeatSteps) {
                RepeatStep repeatStep = (RepeatStep) step;
                @SuppressWarnings("unchecked")
                List<Traversal.Admin> repeatTraversals = repeatStep.getGlobalChildren();
                Traversal.Admin admin = repeatTraversals.get(0);
                @SuppressWarnings("unchecked")
                List<Step> repeatInternalSteps = admin.getSteps();
                collectedRepeatInternalSteps.addAll(repeatInternalSteps);
            }
            if (collectedRepeatInternalSteps.stream().map(s -> s.getClass()).anyMatch(c -> c.equals(RepeatStep.class))) {
                return false;
            } else {
                return collectedRepeatInternalSteps.stream().filter(s -> !s.getClass().equals(RepeatStep.RepeatEndStep.class))
                        .allMatch((s) -> isReplaceableStep(s.getClass()));
            }
        } else {
            return false;
        }
    }

    private List<String> getRestrictedProperties(Step<?, ?> step) {
        List<String> ret = null;
        if (step instanceof PropertiesStep<?> propertiesStep) {
            ret = Arrays.asList(propertiesStep.getPropertyKeys());
        } else if (step instanceof PropertyMapStep<?, ?> propertyMapStep) {
            ret = Arrays.asList(propertyMapStep.getPropertyKeys());
        } else if (step instanceof ElementMapStep<?, ?> elementMapStep) {
            ret = Arrays.asList(elementMapStep.getPropertyKeys());
        }
        // if no property keys are provided, all properties should be returned
        if (ret != null && ret.isEmpty()) {
            ret = null;
        }

        return ret;
    }

    @SuppressWarnings("unchecked")
    protected boolean handleAggregateGlobalStep(ReplacedStep<?, ?> replacedStep, Step aggregateStep, String aggr) {
        replacedStep.setAggregateFunction(org.apache.commons.lang3.tuple.Pair.of(aggr, Collections.emptyList()));
        switch (aggr) {
            case sum -> {
                SqlgSumGlobalStep sqlgSumGlobalStep = new SqlgSumGlobalStep(this.traversal);
                TraversalHelper.replaceStep(aggregateStep, sqlgSumGlobalStep, this.traversal);
            }
            case max -> {
                SqlgMaxGlobalStep sqlgMaxGlobalStep = new SqlgMaxGlobalStep(this.traversal);
                TraversalHelper.replaceStep(aggregateStep, sqlgMaxGlobalStep, this.traversal);
            }
            case min -> {
                SqlgMinGlobalStep sqlgMinGlobalStep = new SqlgMinGlobalStep(this.traversal);
                TraversalHelper.replaceStep(aggregateStep, sqlgMinGlobalStep, this.traversal);
            }
            case "avg" -> {
                SqlgAvgGlobalStep sqlgAvgGlobalStep = new SqlgAvgGlobalStep(this.traversal);
                TraversalHelper.replaceStep(aggregateStep, sqlgAvgGlobalStep, this.traversal);
            }
            case count -> {
                SqlgCountGlobalStep sqlgCountGlobalStep = new SqlgCountGlobalStep(this.traversal);
                TraversalHelper.replaceStep(aggregateStep, sqlgCountGlobalStep, this.traversal);
                int stepIndex = TraversalHelper.stepIndex(sqlgCountGlobalStep, this.traversal);
                SqlgPropertiesStep sqlgPropertiesStep = new SqlgPropertiesStep(
                        this.traversal,
                        PropertyType.VALUE,
                        count
                );
                Set<String> labels = aggregateStep.getLabels();
                for (String label : labels) {
                    sqlgCountGlobalStep.addLabel(label);
                }
                this.traversal.addStep(stepIndex, sqlgPropertiesStep);
            }
            default -> throw new IllegalStateException("Unhandled aggregation " + aggr);
        }
        //aggregate queries do not have select clauses so we clear all the labels
        ReplacedStepTree replacedStepTree = this.currentTreeNodeNode.getReplacedStepTree();
        replacedStepTree.clearLabels();
        return false;
    }

    private boolean handleGroupCountStep(ReplacedStep<?, ?> replacedStep, GroupCountStep<?, ?> groupCountStep) {
        List<? extends Traversal.Admin<?, ?>> localChildren = groupCountStep.getLocalChildren();
        if (localChildren.size() == 1) {
            List<String> groupByKeys = new ArrayList<>();
            Traversal.Admin<?, ?> groupByCountTraversal = localChildren.get(0);
            if (groupByCountTraversal instanceof ValueTraversal) {
                ValueTraversal<?, ?> elementValueTraversal = (ValueTraversal) groupByCountTraversal;
                groupByKeys.add(elementValueTraversal.getPropertyKey());
                replacedStep.getRestrictedProperties().addAll(groupByKeys);
            } else if (groupByCountTraversal instanceof TokenTraversal) {
                TokenTraversal<?, ?> tokenTraversal = (TokenTraversal) groupByCountTraversal;
                if (tokenTraversal.getToken() == T.label) {
                    groupByKeys.add(T.label.getAccessor());
                } else {
                    return false;
                }
            } else {
                return false;
            }
            replacedStep.setGroupBy(groupByKeys);
            replacedStep.setAggregateFunction(org.apache.commons.lang3.tuple.Pair.of(count, groupByKeys));
            SqlgGroupStep<?, ?> sqlgGroupStep = new SqlgGroupStep<>(this.traversal, groupByKeys, GraphTraversal.Symbols.count, false, SqlgGroupStep.REDUCTION.COUNT);
            //noinspection unchecked
            TraversalHelper.replaceStep((Step) groupCountStep, sqlgGroupStep, this.traversal);
            return true;
        } else {
            return false;
        }
    }

    private boolean handleGroupStep(ReplacedStep<?, ?> replacedStep, GroupStep<?, ?, ?> groupStep) {
        List<Traversal.Admin<?, ?>> localChildren = groupStep.getLocalChildren();
        if (localChildren.size() == 2) {
            Traversal.Admin<?, ?> groupByTraversal = localChildren.get(0);
            Traversal.Admin<?, ?> aggregateOverTraversal = localChildren.get(1);
            boolean isPropertiesStep = false;
            List<String> groupByKeys = new ArrayList<>();
            if (groupByTraversal instanceof ValueTraversal) {
                ValueTraversal<?, ?> elementValueTraversal = (ValueTraversal) groupByTraversal;
                groupByKeys.add(elementValueTraversal.getPropertyKey());
            } else if (groupByTraversal instanceof DefaultGraphTraversal) {
                List<Step> groupBySteps = groupByTraversal.getSteps();
                if ((groupBySteps.get(0) instanceof PropertiesStep) || (groupBySteps.get(0) instanceof PropertyMapStep)) {
                    isPropertiesStep = groupBySteps.get(0) instanceof PropertiesStep;
                    List<String> groupBys = getRestrictedProperties(groupBySteps.get(0));
                    groupByKeys.addAll(groupBys);
                } else {
                    return false;
                }
            } else if (groupByTraversal instanceof TokenTraversal) {
                TokenTraversal<?, ?> tokenTraversal = (TokenTraversal) groupByTraversal;
                if (tokenTraversal.getToken() == T.label) {
                    groupByKeys.add(T.label.getAccessor());
                } else {
                    return false;
                }
            } else {
                return false;
            }
            List<Step> valueTraversalSteps = aggregateOverTraversal.getSteps();
            if (valueTraversalSteps.size() == 1) {
                Step one = valueTraversalSteps.get(0);
                if (one instanceof CountGlobalStep) {
                    replacedStep.setAggregateFunction(org.apache.commons.lang3.tuple.Pair.of(GraphTraversal.Symbols.count, List.of()));
                    replacedStep.setGroupBy(groupByKeys);
                    if (!(groupByKeys.size() == 1 && groupByKeys.contains(T.label.getAccessor()))) {
                        replacedStep.getRestrictedProperties().addAll(groupByKeys);
                    }
                    SqlgGroupStep<?, ?> sqlgGroupStep = new SqlgGroupStep<>(this.traversal, groupByKeys, GraphTraversal.Symbols.count, isPropertiesStep, SqlgGroupStep.REDUCTION.COUNT);
                    //noinspection unchecked
                    TraversalHelper.replaceStep((Step) groupStep, sqlgGroupStep, this.traversal);
                }
            } else if (valueTraversalSteps.size() == 2) {
                Step one = valueTraversalSteps.get(0);
                Step two = valueTraversalSteps.get(1);
                if (one instanceof PropertiesStep propertiesStep && two instanceof ReducingBarrierStep) {
                    List<String> aggregationFunctionProperty = getRestrictedProperties(propertiesStep);
                    if (aggregationFunctionProperty == null) {
                        return false;
                    }
                    if (two instanceof CountGlobalStep) {
                        //count should not be used after a property traversal
                        return false;
                    }
                    handlePropertiesStep(replacedStep, propertiesStep, aggregateOverTraversal);

                    replacedStep.getRestrictedProperties().addAll(groupByKeys);
                    replacedStep.setGroupBy(groupByKeys);

                    final SqlgGroupStep.REDUCTION reduction;
                    if (two instanceof MaxGlobalStep) {
                        replacedStep.setAggregateFunction(org.apache.commons.lang3.tuple.Pair.of(GraphTraversal.Symbols.max, aggregationFunctionProperty));
                        reduction = SqlgGroupStep.REDUCTION.MAX;
                    } else if (two instanceof MinGlobalStep) {
                        replacedStep.setAggregateFunction(org.apache.commons.lang3.tuple.Pair.of(GraphTraversal.Symbols.min, aggregationFunctionProperty));
                        reduction = SqlgGroupStep.REDUCTION.MIN;
                    } else if (two instanceof SumGlobalStep) {
                        replacedStep.setAggregateFunction(org.apache.commons.lang3.tuple.Pair.of(sum, aggregationFunctionProperty));
                        reduction = SqlgGroupStep.REDUCTION.SUM;
                    } else if (two instanceof MeanGlobalStep) {
                        replacedStep.setAggregateFunction(org.apache.commons.lang3.tuple.Pair.of("avg", aggregationFunctionProperty));
                        reduction = SqlgGroupStep.REDUCTION.MEAN;
                    } else {
                        throw new IllegalStateException(String.format("Unhandled group by aggregation %s", two.getClass().getSimpleName()));
                    }
                    SqlgGroupStep<?, ?> sqlgGroupStep = new SqlgGroupStep<>(this.traversal, groupByKeys, aggregationFunctionProperty.get(0), isPropertiesStep, reduction);
                    //noinspection unchecked
                    TraversalHelper.replaceStep((Step) groupStep, sqlgGroupStep, this.traversal);
                }
            }
            return false;
        }
        return false;
    }

    private boolean handleNotStep(NotStep<?> notStep, MutableInt pathCount) {
        Step<?, ?> previousStep = notStep.getPreviousStep();
        Traversal.Admin<?, ?> notTraversal = notStep.getLocalChildren().get(0);
        //we can only optimize on one out/in, outE/inE traversals, i.e. predicates on the notTraversal can not be optimized via the anti join LEFT OUTER JOIN technique
        boolean hasOnlyOneVertexAndEdgeSteps = previousStep instanceof SqlgGraphStep<?, ?> &&
                notTraversal.getSteps().size() == 1 &&
                TraversalHelper.hasAllStepsOfClass(notTraversal, VertexStep.class);

        if (hasOnlyOneVertexAndEdgeSteps) {
            List<Step<?, ?>> notTraversalSteps = new ArrayList(notTraversal.getSteps());
            ListIterator<Step<?, ?>> notTraversalStepsIterator = notTraversalSteps.listIterator();
            Step notTraversalStep = notTraversal.getSteps().get(0);
            Preconditions.checkState(notTraversalStep instanceof VertexStep<?>);
            this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
            handleVertexStep(notTraversalStepsIterator, (VertexStep<?>) notTraversalStep, pathCount, true);
            this.currentReplacedStep.setOuterLeftJoin(true);
            int index = TraversalHelper.stepIndex(notStep, this.traversal);
            Preconditions.checkState(index > -1);
            this.traversal.removeStep(notStep);
        }
        return false;
    }

}
