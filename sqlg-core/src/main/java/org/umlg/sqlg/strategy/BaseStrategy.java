package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;
import org.umlg.sqlg.predicate.FullText;
import org.umlg.sqlg.predicate.Text;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.ReplacedStepTree;
import org.umlg.sqlg.structure.SqlgGraph;

import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/04
 */
public abstract class BaseStrategy {

    public static final String PATH_LABEL_SUFFIX = "P~~~";
    public static final String EMIT_LABEL_SUFFIX = "E~~~";
    public static final String SQLG_PATH_FAKE_LABEL = "sqlgPathFakeLabel";
    public static final String SQLG_PATH_ORDER_RANGE_LABEL = "sqlgPathOrderRangeLabel";

    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(
            Compare.eq, Compare.neq, Compare.gt, Compare.gte, Compare.lt, Compare.lte);

    protected Traversal.Admin<?, ?> traversal;
    protected SqlgGraph sqlgGraph;
    private SqlgStep sqlgStep = null;
    private Stack<ReplacedStepTree.TreeNode> chooseStepStack = new Stack<>();
    private ReplacedStepTree.TreeNode currentTreeNodeNode;
    private ReplacedStep<?, ?> currentReplacedStep;
    private boolean continueOptimization = true;

    BaseStrategy(Traversal.Admin<?, ?> traversal) {
        this.traversal = traversal;
        Optional<Graph> graph = traversal.getGraph();
        Preconditions.checkState(graph.isPresent(), "BUG: SqlgGraph must be present on the traversal.");
        //noinspection OptionalGetWithoutIsPresent
        this.sqlgGraph = (SqlgGraph) graph.get();
    }

    void combineSteps() {
        List<Step<?, ?>> steps = new ArrayList(this.traversal.asAdmin().getSteps());
        ListIterator<Step<?, ?>> stepIterator = steps.listIterator();
        MutableInt pathCount = new MutableInt(0);
        while (stepIterator.hasNext()) {
            Step<?, ?> step = stepIterator.next();
            if (this.continueOptimization && isReplaceableStep(step.getClass(), false)) {
                stepIterator.previous();
                handleStep(stepIterator, pathCount);
            } else if (step instanceof SelectStep || (step instanceof SelectOneStep)) {
                collectOrderGlobalSteps(stepIterator, pathCount);
                collectRangeGlobalSteps(stepIterator, pathCount);
                if (stepIterator.hasNext() && stepIterator.next() instanceof SelectOneStep) {
                    break;
                }
            } else {
                //If a step can not be replaced then its the end of optimizationinging.
                break;
            }
        }
        if (this.currentTreeNodeNode != null) {
            this.currentTreeNodeNode.getReplacedStepTree().maybeAddLabelToLeafNodes();
            //If the order is over multiple tables then the resultSet will be completely loaded into memory and then sorted.
            if (this.currentTreeNodeNode.getReplacedStepTree().hasOrderBy()) {
                this.sqlgStep.parseForStrategy();
                if (!this.sqlgStep.isForMultipleQueries() && this.currentTreeNodeNode.getReplacedStepTree().orderByIsOrder()) {
                    this.currentTreeNodeNode.getReplacedStepTree().applyComparatorsOnDb();
                } else {
                    this.sqlgStep.setEagerLoad(true);
                }
            }
            //If a range follows an order that needs to be done in memory then do not apply the range on the db.
            if (this.currentTreeNodeNode.getReplacedStepTree().hasRange()) {
                this.sqlgStep.parseForStrategy();
                if (!this.sqlgStep.isForMultipleQueries() && this.currentTreeNodeNode.getReplacedStepTree().orderByIsOrder()) {
                } else {
                    this.currentTreeNodeNode.getReplacedStepTree().doNotApplyRangeOnDb();
                    this.sqlgStep.setEagerLoad(true);
                }

            }
        }
    }


    /**
     * sqlgStep is either a {@link SqlgGraphStepCompiled} or {@link SqlgVertexStepCompiled}.
     *
     * @return false if optimization must be terminated.
     */
    private void handleStep(ListIterator<Step<?, ?>> stepIterator, MutableInt pathCount) {
        Step<?, ?> step = stepIterator.next();
//        if (step instanceof GraphStep) {
        if (this.sqlgStep == null || step instanceof GraphStep) {
            this.sqlgStep = handleGraphStep(stepIterator, step, pathCount);
        } else {
            Preconditions.checkState(sqlgStep != null);
            if (step instanceof VertexStep || step instanceof EdgeVertexStep || step instanceof EdgeOtherVertexStep) {
                if (!this.chooseStepStack.isEmpty()) {
                    return;
                }
                handleVertexStep(stepIterator, (AbstractStep<?, ?>) step, pathCount);
                //if the chooseStepStack size is greater than the chooseStepNestedCount then it means the just executed
                //handleVertexStep is after nested chooseSteps.
                //This means that this VertexStep applies to the nested chooseSteps where the chooseStep was not chosen.
                //I.e. there was no results for the chooseSteps traversal.
                for (int i = 0; i < this.chooseStepStack.size(); i++) {
                    ReplacedStepTree.TreeNode treeNode = this.chooseStepStack.get(i);
                    this.currentReplacedStep.markAsJoinToLeftJoin();
                    treeNode.addReplacedStep(this.currentReplacedStep);
                }
            } else if (step instanceof RepeatStep) {
                if (!unoptimizableRepeatStep()) {
                    handleRepeatStep((RepeatStep<?>) step, pathCount);
                }
            } else if (step instanceof ChooseStep) {
                if (!unoptimizableChooseStep()) {
                    handleChooseStep(1, (ChooseStep<?, ?, ?>) step, this.traversal, pathCount);
                }
            } else if (step instanceof OrderGlobalStep) {
                stepIterator.previous();
                collectOrderGlobalSteps(stepIterator, pathCount);
                collectRangeGlobalSteps(stepIterator, pathCount);
            } else if (step instanceof RangeGlobalStep) {
                collectRangeGlobalSteps(stepIterator, pathCount);
            } else {
                throw new IllegalStateException("Unhandled step " + step.getClass().getName());
            }
        }
    }

    private SqlgStep handleGraphStep(ListIterator<Step<?, ?>> stepIterator, Step<?, ?> step, MutableInt pathCount) {
        SqlgStep sqlgStep;
        this.currentReplacedStep = ReplacedStep.from(
                this.currentReplacedStep,
                this.sqlgGraph.getTopology(),
                (AbstractStep<?, ?>) step,
                pathCount.getValue()
        );
        collectHasSteps(stepIterator, pathCount.getValue());
        collectOrderGlobalSteps(stepIterator, pathCount);
        collectRangeGlobalSteps(stepIterator, pathCount);
        sqlgStep = constructSqlgStep(step);
        this.currentTreeNodeNode = sqlgStep.addReplacedStep(this.currentReplacedStep);
        replaceStepInTraversal(step, sqlgStep);
        if (sqlgStep instanceof SqlgGraphStepCompiled && ((SqlgGraphStepCompiled) sqlgStep).getIds().length > 0) {
            addHasContainerForIds((SqlgGraphStepCompiled) sqlgStep);
        }
        if (this.currentReplacedStep.getLabels().isEmpty()) {
            boolean precedesPathStep = precedesPathOrTreeStep(this.traversal);
            if (precedesPathStep) {
                this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
            }
        }
        pathCount.increment();
        return sqlgStep;
    }

    private void handleVertexStep(ListIterator<Step<?, ?>> stepIterator, AbstractStep<?, ?> step, MutableInt pathCount) {
        this.currentReplacedStep = ReplacedStep.from(
                this.currentReplacedStep,
                this.sqlgGraph.getTopology(),
                step,
                pathCount.getValue()
        );
        //Important to add the replacedStep before collecting the additional steps.
        //In particular the orderGlobalStep needs to the currentStepDepth setted.
        ReplacedStepTree.TreeNode treeNodeNode = this.sqlgStep.addReplacedStep(this.currentReplacedStep);
        collectHasSteps(stepIterator, pathCount.getValue());
        collectOrderGlobalSteps(stepIterator, pathCount);
        collectRangeGlobalSteps(stepIterator, pathCount);
        //if called from ChooseStep then the VertexStep is nested inside the ChooseStep and not one of the traversal's direct steps.
        int index = TraversalHelper.stepIndex(step, this.traversal);
        if (index != -1) {
            this.traversal.removeStep(step);
        }
        if (this.currentReplacedStep.getLabels().isEmpty()) {
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
        List<Step<?, ?>> repeatStepInternalVertexSteps = admin.getSteps();
        ListIterator<Step<?, ?>> repeatStepIterator = repeatStepInternalVertexSteps.listIterator();
        //this is guaranteed by the previous check unoptimizableRepeat(...)
        LoopTraversal loopTraversal;
        long numberOfLoops;
        loopTraversal = (LoopTraversal) repeatStep.getUntilTraversal();
        numberOfLoops = loopTraversal.getMaxLoops();
        for (int i = 0; i < numberOfLoops; i++) {
            for (Step internalRepeatStep : repeatStepInternalVertexSteps) {
                if (internalRepeatStep instanceof RepeatStep.RepeatEndStep) {
                    break;
                } else if (internalRepeatStep instanceof VertexStep || internalRepeatStep instanceof EdgeVertexStep || internalRepeatStep instanceof EdgeOtherVertexStep) {
                    ReplacedStep<?, ?> replacedStepToEmit;
                    //this means the ReplacedStep before the RepeatStep need to be emitted.
                    //i.e. the currentReplacedStep before running handleVertexStep needs to be emitted.
                    if (repeatStep.emitFirst) {
                        replacedStepToEmit = this.currentReplacedStep;
                        pathCount.decrement();
                        //noinspection ConstantConditions
                        replacedStepToEmit.setEmit(repeatStep.getEmitTraversal() != null);
                        replacedStepToEmit.setUntilFirst(repeatStep.untilFirst);
                        if (repeatStep.getLabels().isEmpty()) {
                            replacedStepToEmit.addLabel(pathCount + BaseStrategy.EMIT_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
                        } else {
                            for (String label : repeatStep.getLabels()) {
                                replacedStepToEmit.addLabel(pathCount + BaseStrategy.EMIT_LABEL_SUFFIX + label);
                            }
                        }
                        pathCount.increment();
                    }
                    handleVertexStep(repeatStepIterator, (AbstractStep<?, ?>) internalRepeatStep, pathCount);
                    pathCount.decrement();
                    if (!repeatStep.emitFirst) {
                        replacedStepToEmit = this.currentReplacedStep;
                        //noinspection ConstantConditions
                        replacedStepToEmit.setEmit(repeatStep.getEmitTraversal() != null);
                        replacedStepToEmit.setUntilFirst(repeatStep.untilFirst);
                        if (repeatStep.getLabels().isEmpty()) {
                            replacedStepToEmit.addLabel(pathCount + BaseStrategy.EMIT_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_FAKE_LABEL);
                        } else {
                            for (String label : repeatStep.getLabels()) {
                                replacedStepToEmit.addLabel(pathCount + BaseStrategy.EMIT_LABEL_SUFFIX + label);
                            }
                        }
                    }
                    pathCount.increment();
                } else {
                    throw new IllegalStateException("Unhandled step nested in RepeatStep " + internalRepeatStep.getClass().getName());
                }
            }
        }
        this.traversal.removeStep(repeatStep);
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
        List<Step<?, ?>> trueTraversalSteps = new ArrayList(trueTraversal.getSteps());
        ListIterator<Step<?, ?>> trueTraversalStepsIterator = trueTraversalSteps.listIterator();
        while (trueTraversalStepsIterator.hasNext()) {
            Step internalChooseStep = trueTraversalStepsIterator.next();
            if (internalChooseStep instanceof VertexStep || internalChooseStep instanceof EdgeVertexStep || internalChooseStep instanceof EdgeOtherVertexStep) {
                handleVertexStep(trueTraversalStepsIterator, (AbstractStep<?, ?>) internalChooseStep, pathCount);
                //if the chooseStepStack size is greater than the chooseStepNestedCount then it means the just executed
                //handleVertexStep is after nested chooseSteps.
                //This means that this VertexStep applies to the nested chooseSteps where the chooseStep was not chosen.
                //I.e. there was no results for the chooseSteps traversal.
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

    protected abstract boolean isReplaceableStep(Class<? extends Step> stepClass, boolean alreadyReplacedGraphStep);

    protected abstract void replaceStepInTraversal(Step stepToReplace, SqlgStep sqlgStep);

    private void collectHasSteps(ListIterator<Step<?, ?>> iterator, int pathCount) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) currentStep;
                List<HasContainer> hasContainers = hasContainerHolder.getHasContainers();
                List<HasContainer> toRemoveHasContainers = new ArrayList<>();
                if (isNotZonedDateTimeOrPeriodOrDuration(hasContainerHolder)) {
                    toRemoveHasContainers.addAll(optimizeHas(this.currentReplacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeWithInOut(this.currentReplacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeBetween(this.currentReplacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeInside(this.currentReplacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeOutside(this.currentReplacedStep, hasContainers));
                    toRemoveHasContainers.addAll(optimizeTextContains(this.currentReplacedStep, hasContainers));
                    if (toRemoveHasContainers.size() == hasContainers.size()) {
                        if (!currentStep.getLabels().isEmpty()) {
                            final IdentityStep identityStep = new IdentityStep<>(this.traversal);
                            currentStep.getLabels().forEach(l -> this.currentReplacedStep.addLabel(pathCount + BaseStrategy.PATH_LABEL_SUFFIX + l));
                            TraversalHelper.insertAfterStep(identityStep, currentStep, this.traversal);
                        }
                        if (this.traversal.getSteps().contains(currentStep)) {
                            this.traversal.removeStep(currentStep);
                        }
                        iterator.remove();
                    }
                }
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }

    private void collectOrderGlobalSteps(ListIterator<Step<?, ?>> iterator, MutableInt pathCount) {
        //Collect the OrderGlobalSteps
        while (iterator.hasNext()) {
            Step<?, ?> step = iterator.next();
            if (step instanceof OrderGlobalStep) {
                if (optimizable((OrderGlobalStep) step)) {
                    //The step might not be here. For instance if it was nested in a chooseStep where the chooseStep logic already removed the step.
                    if (this.traversal.getSteps().contains(step)) {
                        this.traversal.removeStep(step);
                    }
                    iterator.previous();
                    Step previousStep = iterator.previous();
                    if (previousStep instanceof SelectOneStep) {
                        SelectOneStep selectOneStep = (SelectOneStep) previousStep;
                        String key = (String) selectOneStep.getScopeKeys().iterator().next();
                        this.currentReplacedStep.getSqlgComparatorHolder().setPrecedingSelectOneLabel(key);
                    }
                    iterator.next();
                    iterator.next();
                    List<Pair<Traversal.Admin<?, ?>, Comparator<?>>> comparators = ((OrderGlobalStep) step).getComparators();
                    this.currentReplacedStep.getSqlgComparatorHolder().setComparators(comparators);
                    this.currentReplacedStep.getSqlgComparatorHolder().setReplacedStepDepth(this.currentReplacedStep.getDepth());
                    //add a label if the step does not yet have one and is not a leaf node
                    if (this.currentReplacedStep.getLabels().isEmpty()) {
                        this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL);
                    }
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

    private boolean optimizable(OrderGlobalStep step) {
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

    private void collectRangeGlobalSteps(ListIterator<Step<?, ?>> iterator, MutableInt pathCount) {
        //Collect the OrderGlobalSteps
        while (iterator.hasNext()) {
            Step<?, ?> step = iterator.next();
            if (step instanceof RangeGlobalStep) {
                //The step might not be here. For instance if it was nested in a chooseStep where the chooseStep logic already removed the step.
                if (this.traversal.getSteps().contains(step)) {
                    this.traversal.removeStep(step);
                }
                RangeGlobalStep<?> rgs = (RangeGlobalStep<?>) step;
                long high = rgs.getHighRange();
                this.currentReplacedStep.setSqlgRangeHolder(SqlgRangeHolder.from(Range.between(rgs.getLowRange(), high)));
                //add a label if the step does not yet have one and is not a leaf node
                if (this.currentReplacedStep.getLabels().isEmpty()) {
                    this.currentReplacedStep.addLabel(pathCount.getValue() + BaseStrategy.PATH_LABEL_SUFFIX + BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL);
                }
                this.continueOptimization = false;
            } else {
                //break on the first step that is not a RangeGlobalStep
                iterator.previous();
                break;
            }
        }
    }

    private static boolean precedesPathOrTreeStep(Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() != null && traversal.getParent() instanceof LocalStep) {
            LocalStep localStep = (LocalStep) traversal.getParent();
            if (precedesPathOrTreeStep(localStep.getTraversal())) {
                return true;
            }
        }
        Predicate p = s -> s.getClass().equals(PathStep.class) ||
                s.getClass().equals(TreeStep.class) ||
                s.getClass().equals(TreeSideEffectStep.class) ||
                s.getClass().equals(CyclicPathStep.class) ||
                s.getClass().equals(SimplePathStep.class) ||
                s.getClass().equals(EdgeOtherVertexStep.class);
        return TraversalHelper.anyStepRecursively(p, traversal);
    }

    private void addHasContainerForIds(SqlgGraphStepCompiled sqlgGraphStepCompiled) {
        Object[] ids = sqlgGraphStepCompiled.getIds();
        List<Object> recordsIds = new ArrayList<>();
        for (Object id : ids) {
            if (id instanceof Element) {
                recordsIds.add(((Element) id).id());
            } else {
                recordsIds.add(id);
            }
        }
        HasContainer idHasContainer = new HasContainer(T.id.getAccessor(), P.within(recordsIds.toArray()));
        this.currentReplacedStep.addHasContainers(Collections.singletonList(idHasContainer));
        sqlgGraphStepCompiled.clearIds();
    }

    protected boolean isNotZonedDateTimeOrPeriodOrDuration(HasContainerHolder currentStep) {
        for (HasContainer h : currentStep.getHasContainers()) {
            P<?> predicate = h.getPredicate();
            if (predicate.getValue() instanceof ZonedDateTime ||
                    predicate.getValue() instanceof Period ||
                    predicate.getValue() instanceof Duration ||
                    (predicate.getValue() instanceof List && containsZonedDateTimePeriodOrDuration((List<Object>) predicate.getValue())) ||
                    (predicate instanceof ConnectiveP && isConnectivePWithZonedDateTimePeriodOrDuration((ConnectiveP) h.getPredicate()))) {


                return false;
            }

        }
        return true;
    }

    private List<HasContainer> optimizeHas(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (SUPPORTED_BI_PREDICATE.contains(hasContainer.getBiPredicate())) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private List<HasContainer> optimizeWithInOut(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (!hasContainer.getKey().equals(T.label.getAccessor()) &&
                    (hasContainer.getBiPredicate() == Contains.without || hasContainer.getBiPredicate() == Contains.within)) {

                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }


    private List<HasContainer> optimizeBetween(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getPredicate() instanceof AndP) {
                AndP<?> andP = (AndP) hasContainer.getPredicate();
                List<? extends P<?>> predicates = andP.getPredicates();
                if (predicates.size() == 2) {
                    if (predicates.get(0).getBiPredicate() == Compare.gte && predicates.get(1).getBiPredicate() == Compare.lt) {
                        replacedStep.addHasContainer(hasContainer);
                        result.add(hasContainer);
                    }
                }
            }
        }
        return result;
    }

    private List<HasContainer> optimizeInside(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getPredicate() instanceof AndP) {
                AndP<?> andP = (AndP) hasContainer.getPredicate();
                List<? extends P<?>> predicates = andP.getPredicates();
                if (predicates.size() == 2) {
                    if (predicates.get(0).getBiPredicate() == Compare.gt && predicates.get(1).getBiPredicate() == Compare.lt) {
                        replacedStep.addHasContainer(hasContainer);
                        result.add(hasContainer);
                    }
                }
            }
        }
        return result;
    }

    private List<HasContainer> optimizeOutside(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getPredicate() instanceof OrP) {
                OrP<?> orP = (OrP) hasContainer.getPredicate();
                List<? extends P<?>> predicates = orP.getPredicates();
                if (predicates.size() == 2) {
                    if (predicates.get(0).getBiPredicate() == Compare.lt && predicates.get(1).getBiPredicate() == Compare.gt) {
                        replacedStep.addHasContainer(hasContainer);
                        result.add(hasContainer);
                    }
                }
            }
        }
        return result;
    }

    private List<HasContainer> optimizeTextContains(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
        List<HasContainer> result = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getBiPredicate() instanceof Text ||
                    hasContainer.getBiPredicate() instanceof FullText
                    ) {
                replacedStep.addHasContainer(hasContainer);
                result.add(hasContainer);
            }
        }
        return result;
    }

    private boolean containsZonedDateTimePeriodOrDuration(List<Object> values) {
        for (Object value : values) {
            if (value instanceof ZonedDateTime ||
                    value instanceof Period ||
                    value instanceof Duration) {
                return true;
            }
        }
        return false;
    }

    private boolean isConnectivePWithZonedDateTimePeriodOrDuration(ConnectiveP connectiveP) {
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
        final List<Step<?, ?>> steps = new ArrayList(this.traversal.asAdmin().getSteps());
        final ListIterator<Step<?, ?>> stepIterator = steps.listIterator();
        List<Step<?, ?>> toCome = steps.subList(stepIterator.nextIndex(), steps.size());
        return toCome.stream().anyMatch(s ->
                s.getClass().equals(Order.class) ||
                        s.getClass().equals(LambdaCollectingBarrierStep.class) ||
                        s.getClass().equals(SackValueStep.class) ||
                        s.getClass().equals(SackStep.class));
    }

    private boolean unoptimizableChooseStep() {
        List<ChooseStep<?, ?, ?>> chooseSteps = new ArrayList(TraversalHelper.getStepsOfAssignableClassRecursively(ChooseStep.class, this.traversal.asAdmin()));
        for (ChooseStep<?, ?, ?> chooseStep : chooseSteps) {
            List<? extends Traversal.Admin<?, ?>> traversalAdmins = chooseStep.getGlobalChildren();
            if (traversalAdmins.size() != 2) {
                return true;
            }
            Traversal.Admin<?, ?> predicate = chooseStep.getLocalChildren().get(0);
            List<Step> predicateSteps = new ArrayList<>(predicate.getSteps());
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
        }
        return false;
    }

    private boolean unoptimizableRepeatStep() {
        List<RepeatStep> repeatSteps = TraversalHelper.getStepsOfAssignableClassRecursively(RepeatStep.class, this.traversal);
        boolean hasUntil = repeatSteps.stream().filter(s -> s.getClass().equals(RepeatStep.class)).allMatch(repeatStep -> repeatStep.getUntilTraversal() != null);
        boolean hasUnoptimizableUntil = false;
        if (hasUntil) {
            hasUnoptimizableUntil = repeatSteps.stream().filter(s -> s.getClass().equals(RepeatStep.class)).allMatch(repeatStep -> !(repeatStep.getUntilTraversal() instanceof LoopTraversal));
        }
        boolean badRepeat = !hasUntil || hasUnoptimizableUntil;
        //Check if the repeat step only contains optimizable steps
        if (!badRepeat) {
            List<Step> collectedRepeatInternalSteps = new ArrayList<>();
            for (Step step : repeatSteps) {
                RepeatStep repeatStep = (RepeatStep) step;
                List<Traversal.Admin> repeatTraversals = repeatStep.<Traversal.Admin>getGlobalChildren();
                Traversal.Admin admin = repeatTraversals.get(0);
                List<Step> repeatInternalSteps = admin.getSteps();
                collectedRepeatInternalSteps.addAll(repeatInternalSteps);
            }
            return !collectedRepeatInternalSteps.stream().filter(s -> !s.getClass().equals(RepeatStep.RepeatEndStep.class))
                    .allMatch((s) -> isReplaceableStep(s.getClass(), false));
        } else {
            return true;
        }
    }

}
