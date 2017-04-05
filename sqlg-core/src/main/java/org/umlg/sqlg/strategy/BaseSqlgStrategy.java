package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

/**
 * Created by pieter on 2015/07/19.
 */
public abstract class BaseSqlgStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

//    protected Logger logger = LoggerFactory.getLogger(getClass().getName());
//    public static final String PATH_LABEL_SUFFIX = "P~~~";
//    public static final String EMIT_LABEL_SUFFIX = "E~~~";
//    private static final String SQLG_PATH_FAKE_LABEL = "sqlgPathFakeLabel";
//    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(
//            Compare.eq, Compare.neq, Compare.gt, Compare.gte, Compare.lt, Compare.lte);
//    protected static List<Class> UNOPTIMIZABLE_STEPS = Arrays.asList(
//            Order.class, LambdaCollectingBarrierStep.class, SackValueStep.class, SackStep.class);
//
//    BaseSqlgStrategy() {
//    }
//
//    protected abstract void replaceStepInTraversal(Step stepToReplace, SqlgStep sqlgStep, Traversal.Admin<?, ?> traversal);
//
//    protected abstract void doLastEntry(Step step, ListIterator<Step> stepIterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> lastReplacedStep, SqlgStep sqlgStep, int pathCount);
//
//    protected abstract boolean isReplaceableStep(Class<? extends Step> stepClass, boolean alreadyReplacedGraphStep);
//
//    protected abstract SqlgStep constructSqlgStep(Traversal.Admin<?, ?> traversal, Step startStep);
//
//    void combineSteps(Traversal.Admin<?, ?> traversal, List<Step> steps, ListIterator<Step> stepIterator) {
//        //Replace all consecutive VertexStep and HasStep with one step
//        SqlgStep sqlgStep = null;
//        Step previous = null;
//        ReplacedStep<?, ?> lastReplacedStep = null;
//
//        int pathCount = 0;
//        boolean alreadyReplacedGraphStep = false;
//        boolean repeatStepAdded;
//        boolean chooseStepAdded = false;
//        MutableInt repeatStepsAdded = new MutableInt(0);
//        while (stepIterator.hasNext()) {
//            Step step = stepIterator.next();
//
//            //Check for RepeatStep(s) and insert them into the stepIterator
//            if (step instanceof RepeatStep) {
//                try {
//                    //flatten the RepeatStep's repetitions into the stepIterator
//                    repeatStepAdded = flattenRepeatStep(steps, stepIterator, (RepeatStep) step, traversal, repeatStepsAdded);
//                    //This sucks again, this logic is for times(0) where the incoming vertex needs to be returned.
//                    if (this instanceof SqlgVertexStepStrategy && !repeatStepAdded) {
//                        traversal.addStep(new IdentityStep<>(traversal));
//                    }
//                } catch (UnoptimizableException e) {
//                    //swallow as it is used for process flow only.
//                    //noinspection ConstantConditions
//                    lastReplacedStep.addLabel((pathCount) + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
//                    return;
//                }
//            } else if (step instanceof ChooseStep) {
//                try {
//                    //Only optimize ChooseSteps where the predicate is the same as the true traversal
//                    chooseStepAdded = flattenChooseStep(steps, stepIterator, (ChooseStep) step, traversal);
//                } catch (UnoptimizableException e) {
//                    //swallow as it is used for process flow only.
//                    return;
//                }
//            } else {
//
//                if (isReplaceableStep(step.getClass(), alreadyReplacedGraphStep)) {
//
//                    //check if repeat steps were added to the stepIterator
//                    boolean emit = false;
//                    boolean emitFirst = false;
//                    boolean untilFirst = false;
//                    Set<String> labels = new HashSet<>();
//                    if (repeatStepsAdded.getValue() > 0) {
//                        repeatStepsAdded.decrement();
//                        RepeatStep repeatStep = (RepeatStep) step.getTraversal().getParent();
//                        emit = repeatStep.getEmitTraversal() != null;
//                        emitFirst = repeatStep.emitFirst;
//                        untilFirst = repeatStep.untilFirst;
//                        labels.addAll(repeatStep.getLabels());
//                    }
//
//                    pathCount++;
//
//                    @SuppressWarnings("OptionalGetWithoutIsPresent")
//                    ReplacedStep replacedStep = ReplacedStep.from(((SqlgGraph) traversal.getGraph().get()).getTopology(), (AbstractStep) step, pathCount);
//                    if (sqlgStep == null || step instanceof GraphStep) {
//                        sqlgStep = constructSqlgStep(traversal, step);
//                        if (previous != null) {
//                            sqlgStep.setPreviousStep(previous);
//                        }
//                        alreadyReplacedGraphStep = alreadyReplacedGraphStep || step instanceof GraphStep;
//                        //TODO this suck but brain is stuck.
//                        if (this instanceof SqlgGraphStepStrategy) {
//                            sqlgStep.addReplacedStep(replacedStep);
//                        } else if (this instanceof SqlgVertexStepStrategy) {
//                            previous = step;
//                        } else {
//                            throw new IllegalStateException("Unknown strategy " + this.getClass().getName());
//                        }
//                        replaceStepInTraversal(step, sqlgStep, traversal);
//                        if (sqlgStep instanceof SqlgGraphStepCompiled && ((SqlgGraphStepCompiled) sqlgStep).getIds().length > 0) {
//                            addHasContainerForIds((SqlgGraphStepCompiled) sqlgStep, replacedStep);
//                        }
//                        handleHasSteps(stepIterator, traversal, replacedStep, pathCount);
//                    }
//
//                    if (emit) {
//                        //the previous step must be marked as emit.
//                        //this is because emit() before repeat() indicates that the incoming element for every repeat must be emitted.
//                        //i.e. g.V().hasLabel('A').emit().repeat(out('b', 'c')) means A and B must be emitted
//                        List<ReplacedStep> previousReplacedSteps = sqlgStep.getReplacedSteps();
//                        ReplacedStep previousReplacedStep;
//                        if (emitFirst) {
//                            previousReplacedStep = previousReplacedSteps.get(previousReplacedSteps.size() - 1);
//                            pathCount--;
//                        } else {
//                            previousReplacedStep = replacedStep;
//                        }
//                        previousReplacedStep.setEmit(true);
//                        previousReplacedStep.setUntilFirst(untilFirst);
//                        if (labels.isEmpty()) {
//                            previousReplacedStep.addLabel(pathCount + BaseSqlgStrategy.EMIT_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
//                        } else {
//                            for (String label : labels) {
//                                previousReplacedStep.addLabel(pathCount + BaseSqlgStrategy.EMIT_LABEL_SUFFIX + label);
//                            }
//                        }
//                        //Remove the path label if there is one. No need for 2 labels as emit labels go onto the path anyhow.
//                        previousReplacedStep.getLabels().remove(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
//                        if (emitFirst) {
//                            pathCount++;
//                        }
//                    }
//                    if (chooseStepAdded) {
//                        pathCount--;
//                        List<ReplacedStep> previousReplacedSteps = sqlgStep.getReplacedSteps();
//                        ReplacedStep previousReplacedStep = previousReplacedSteps.get(previousReplacedSteps.size() - 1);
//                        previousReplacedStep.setLeftJoin(true);
//                        //Remove the path label if there is one. No need for 2 labels.
//                        previousReplacedStep.getLabels().remove(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
//                        previousReplacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
//                        pathCount++;
//                    }
//                    if (replacedStep.getLabels().isEmpty()) {
//                        boolean precedesPathStep = precedesPathOrTreeStep(traversal);
//                        if (precedesPathStep) {
//                            replacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
//                        }
//                    }
//                    if (previous != null && !(step instanceof GraphStep)) {
//                        sqlgStep.addReplacedStep(replacedStep);
//                        int index = TraversalHelper.stepIndex(step, traversal);
//                        if (index != -1) {
//                            traversal.removeStep(step);
//                        }
//                        handleHasSteps(stepIterator, traversal, replacedStep, pathCount);
//                    }
//                    previous = step;
//                    lastReplacedStep = replacedStep;
//                    chooseStepAdded = false;
//                } else {
//                    if (lastReplacedStep != null) {
//                        boolean doLastEntry = step instanceof OrderGlobalStep || step instanceof RangeGlobalStep;
//                        if (!doLastEntry && (step instanceof SelectStep || step instanceof SelectOneStep || step instanceof EmptyStep) && stepIterator.hasNext()) {
//                            Step<?, ?> nextStep = stepIterator.next();
//                            doLastEntry = nextStep instanceof OrderGlobalStep || nextStep instanceof RangeGlobalStep;
//                            stepIterator.previous();
//                        }
//                        if (doLastEntry) {
//                            doLastEntry(step, stepIterator, traversal, lastReplacedStep, sqlgStep,pathCount);
//                        }
//                    }
////                    if (lastReplacedStep != null && steps.stream().anyMatch(s -> s instanceof OrderGlobalStep || s instanceof RangeGlobalStep)) {
////                        doLastEntry(step, stepIterator, traversal, lastReplacedStep, sqlgStep);
////                    }
//                    break;
//                }
//            }
//        }
//        if (lastReplacedStep != null && !lastReplacedStep.isEmit() && lastReplacedStep.getLabels().isEmpty()) {
//            lastReplacedStep.addLabel((pathCount) + BaseSqlgStrategy.PATH_LABEL_SUFFIX + BaseSqlgStrategy.SQLG_PATH_FAKE_LABEL);
//        }
//    }
//
//    private boolean unoptimizableChooseStep(List<Step> steps, int index) {
//        List<Step> toCome = steps.subList(index, steps.size());
//        Step step = toCome.get(0);
//        Preconditions.checkState(step instanceof ChooseStep, "Expected ChooseStep, found " + step.getClass().getSimpleName() + " instead. BUG!");
//        ChooseStep chooseStep = (ChooseStep) step;
//
//
//        List<Traversal.Admin<?, ?>> traversalAdmins = chooseStep.getGlobalChildren();
//        if (traversalAdmins.size() != 2) {
//            return true;
//        }
//
//        Traversal.Admin<?, ?> predicate = (Traversal.Admin<?, ?>) chooseStep.getLocalChildren().get(0);
//        List<Step> predicateSteps = new ArrayList<>(predicate.getSteps());
//
//        if (predicateSteps.stream().anyMatch(s -> s instanceof OrderGlobalStep)) {
//            return true;
//        }
//
//        predicateSteps.remove(predicate.getSteps().size() - 1);
//
//        Traversal.Admin<?, ?> globalChildOne = (Traversal.Admin<?, ?>) chooseStep.getGlobalChildren().get(0);
//        List<Step> globalChildOneSteps = new ArrayList<>(globalChildOne.getSteps());
//        globalChildOneSteps.remove(globalChildOneSteps.size() - 1);
//
//        Traversal.Admin<?, ?> globalChildTwo = (Traversal.Admin<?, ?>) chooseStep.getGlobalChildren().get(1);
//        List<Step> globalChildTwoSteps = new ArrayList<>(globalChildTwo.getSteps());
//        globalChildTwoSteps.remove(globalChildTwoSteps.size() - 1);
//
//
//        boolean hasIdentity = globalChildOne.getSteps().stream().anyMatch(s -> s instanceof IdentityStep);
//        if (!hasIdentity) {
//            hasIdentity = globalChildTwo.getSteps().stream().anyMatch(s -> s instanceof IdentityStep);
//            if (hasIdentity) {
//                //Identity found check predicate and true are the same
//                if (!predicateSteps.equals(globalChildOneSteps)) {
//                    return true;
//                }
//            } else {
//                //Identity not found
//                return true;
//            }
//        } else {
//            //Identity found check predicate and true are the same
//            if (!predicateSteps.equals(globalChildTwoSteps)) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    protected boolean unoptimizableRepeat(List<Step> steps, int index) {
//        List<Step> toCome = steps.subList(index, steps.size());
//        boolean repeatExist = toCome.stream().anyMatch(s -> s.getClass().equals(RepeatStep.class));
//        if (repeatExist) {
//            boolean hasUntil = toCome.stream().filter(s -> s.getClass().equals(RepeatStep.class)).allMatch(r -> {
//                RepeatStep repeatStep = (RepeatStep) r;
//                return repeatStep.getUntilTraversal() != null;
//            });
//            boolean hasUnoptimizableUntil = false;
//            if (hasUntil) {
//                hasUnoptimizableUntil = toCome.stream().filter(s -> s.getClass().equals(RepeatStep.class)).allMatch(r -> {
//                    RepeatStep repeatStep = (RepeatStep) r;
//                    return !(repeatStep.getUntilTraversal() instanceof LoopTraversal);
//                });
//            }
//            boolean badRepeat = !hasUntil || hasUnoptimizableUntil;
//            //Check if the repeat step only contains optimizable steps
//            if (!badRepeat) {
//                List<Step> collectedRepeatInternalSteps = new ArrayList<>();
//                List<Step> repeatSteps = toCome.stream().filter(s -> s.getClass().equals(RepeatStep.class)).collect(Collectors.toList());
//                for (Step step : repeatSteps) {
//                    RepeatStep repeatStep = (RepeatStep) step;
//                    List<Traversal.Admin> repeatTraversals = repeatStep.<Traversal.Admin>getGlobalChildren();
//                    Traversal.Admin admin = repeatTraversals.get(0);
//                    List<Step> repeatInternalSteps = admin.getSteps();
//                    collectedRepeatInternalSteps.addAll(repeatInternalSteps);
//                }
//                return !collectedRepeatInternalSteps.stream().filter(s -> !s.getClass().equals(RepeatStep.RepeatEndStep.class))
//                        .allMatch((s) -> isReplaceableStep(s.getClass(), false));
//            } else {
//                return true;
//            }
//        } else {
//            return false;
//        }
//    }
//
//    boolean canNotBeOptimized(List<Step> steps, int index) {
//        List<Step> toCome = steps.subList(index, steps.size());
//        return toCome.stream().anyMatch(s ->
//                s.getClass().equals(Order.class) ||
//                        s.getClass().equals(LambdaCollectingBarrierStep.class) ||
//                        s.getClass().equals(SackValueStep.class) ||
//                        s.getClass().equals(SackStep.class));
//    }
//
//    boolean canNotBeOptimized(final Traversal.Admin<?, ?> traversal) {
//        for (Class unoptimizableStep : UNOPTIMIZABLE_STEPS) {
//            if (!TraversalHelper.getStepsOfAssignableClassRecursively(unoptimizableStep, traversal).isEmpty()) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    private boolean precedesPathOrTreeStep(Traversal.Admin<?, ?> traversal) {
//        if (traversal.getParent() != null && traversal.getParent() instanceof LocalStep) {
//            LocalStep localStep = (LocalStep) traversal.getParent();
//            if (precedesPathOrTreeStep(localStep.getTraversal())) {
//                return true;
//            }
//        }
//        Predicate p = s -> s.getClass().equals(PathStep.class) ||
//                s.getClass().equals(TreeStep.class) ||
//                s.getClass().equals(TreeSideEffectStep.class) ||
//                s.getClass().equals(CyclicPathStep.class) ||
//                s.getClass().equals(SimplePathStep.class) ||
//                s.getClass().equals(EdgeOtherVertexStep.class);
//        return TraversalHelper.anyStepRecursively(p, traversal);
//    }
//
//    private void handleHasSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, ReplacedStep<?, ?> replacedStep, int pathCount) {
//        //Collect the hasSteps
//        while (iterator.hasNext()) {
//            Step<?, ?> currentStep = iterator.next();
//            if (currentStep instanceof HasContainerHolder) {
//                HasContainerHolder hasContainerHolder = (HasContainerHolder) currentStep;
//                List<HasContainer> hasContainers = hasContainerHolder.getHasContainers();
//                List<HasContainer> toRemoveHasContainers = new ArrayList<>();
//                if (isNotZonedDateTimeOrPeriodOrDuration(hasContainerHolder)) {
//                    toRemoveHasContainers.addAll(optimizeHas(replacedStep, hasContainers));
//                    toRemoveHasContainers.addAll(optimizeWithInOut(replacedStep, hasContainers));
//                    toRemoveHasContainers.addAll(optimizeBetween(replacedStep, hasContainers));
//                    toRemoveHasContainers.addAll(optimizeInside(replacedStep, hasContainers));
//                    toRemoveHasContainers.addAll(optimizeOutside(replacedStep, hasContainers));
//                    toRemoveHasContainers.addAll(optimizeTextContains(replacedStep, hasContainers));
//                    if (toRemoveHasContainers.size() == hasContainers.size()) {
//                        if (!currentStep.getLabels().isEmpty()) {
//                            final IdentityStep identityStep = new IdentityStep<>(traversal);
//                            currentStep.getLabels().forEach(l -> replacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + l));
//                            TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
//                        }
//                        if (traversal.getSteps().contains(currentStep)) {
//                            traversal.removeStep(currentStep);
//                        }
//                        iterator.remove();
//                    }
//                }
//            } else if (currentStep instanceof IdentityStep) {
//                // do nothing
//            } else {
//                iterator.previous();
//                break;
//            }
//
////            if (currentStep instanceof HasContainerHolder && isNotZonedDateTimeOrPeriodOrDuration((HasContainerHolder) currentStep) &&
////                    (isSingleBiPredicate(((HasContainerHolder) currentStep).getHasContainers()) ||
////                            isBetween(((HasContainerHolder) currentStep).getHasContainers()) ||
////                            isInside(((HasContainerHolder) currentStep).getHasContainers()) ||
////                            isOutside(((HasContainerHolder) currentStep).getHasContainers()) ||
////                            isWithinOut(((HasContainerHolder) currentStep).getHasContainers()) ||
////                            isTextContains(((HasContainerHolder) currentStep).getHasContainers()))) {
////
////                if (!currentStep.getLabels().isEmpty()) {
////                    final IdentityStep identityStep = new IdentityStep<>(traversal);
////                    currentStep.getLabels().forEach(l -> replacedStep.addLabel(pathCount + BaseSqlgStrategy.PATH_LABEL_SUFFIX + l));
////                    TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
////                }
////                iterator.remove();
////                //TODO strengthen this if statement.
////                //The step is not present for ChooseSteps as the currentStep is nested inside the ChooserStep which has already been removed.
////                //The same should be true for RepeatStep only currently do not optimize if there is a HasStep in the nested traversal.
////                if (traversal.getSteps().contains(currentStep)) {
////                    traversal.removeStep(currentStep);
////                }
////                replacedStep.addHasContainers(((HasContainerHolder) currentStep).getHasContainers());
////            } else if (currentStep instanceof IdentityStep) {
////                // do nothing
////            } else {
////                iterator.previous();
////                break;
////            }
//        }
//    }
//
//    protected boolean isNotZonedDateTimeOrPeriodOrDuration(HasContainerHolder currentStep) {
//        for (HasContainer h : currentStep.getHasContainers()) {
//            P<?> predicate = h.getPredicate();
//            if (predicate.getValue() instanceof ZonedDateTime ||
//                    predicate.getValue() instanceof Period ||
//                    predicate.getValue() instanceof Duration ||
//                    (predicate.getValue() instanceof List && containsZonedDateTimePeriodOrDuration((List<Object>) predicate.getValue())) ||
//                    (predicate instanceof ConnectiveP && isConnectivePWithZonedDateTimePeriodOrDuration((ConnectiveP) h.getPredicate()))) {
//
//
//                return false;
//            }
//
//        }
//        return true;
//    }
//
//    private boolean containsZonedDateTimePeriodOrDuration(List<Object> values) {
//        for (Object value : values) {
//            if (value instanceof ZonedDateTime ||
//                    value instanceof Period ||
//                    value instanceof Duration) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    private boolean isConnectivePWithZonedDateTimePeriodOrDuration(ConnectiveP connectiveP) {
//        List<P<?>> ps = connectiveP.getPredicates();
//        for (P<?> predicate : ps) {
//            if (predicate.getValue() instanceof ZonedDateTime ||
//                    predicate.getValue() instanceof Period ||
//                    predicate.getValue() instanceof Duration) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    private List<HasContainer> optimizeWithInOut(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
//        List<HasContainer> result = new ArrayList<>();
//        for (HasContainer hasContainer : hasContainers) {
//            if (!hasContainer.getKey().equals(T.label.getAccessor()) &&
//                    (hasContainer.getBiPredicate() == Contains.without || hasContainer.getBiPredicate() == Contains.within)) {
//
//                replacedStep.addHasContainer(hasContainer);
//                result.add(hasContainer);
//            }
//        }
//        return result;
//    }
//
//    private List<HasContainer> optimizeHas(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
//        List<HasContainer> result = new ArrayList<>();
//        for (HasContainer hasContainer : hasContainers) {
//            if (SUPPORTED_BI_PREDICATE.contains(hasContainer.getBiPredicate())) {
//                replacedStep.addHasContainer(hasContainer);
//                result.add(hasContainer);
//            }
//        }
//        return result;
//    }
//
//    private List<HasContainer> optimizeBetween(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
//        List<HasContainer> result = new ArrayList<>();
//        for (HasContainer hasContainer : hasContainers) {
//            if (hasContainer.getPredicate() instanceof AndP) {
//                AndP<?> andP = (AndP) hasContainer.getPredicate();
//                List<? extends P<?>> predicates = andP.getPredicates();
//                if (predicates.size() == 2) {
//                    if (predicates.get(0).getBiPredicate() == Compare.gte && predicates.get(1).getBiPredicate() == Compare.lt) {
//                        replacedStep.addHasContainer(hasContainer);
//                        result.add(hasContainer);
//                    }
//                }
//            }
//        }
//        return result;
//    }
//
//    private List<HasContainer> optimizeInside(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
//        List<HasContainer> result = new ArrayList<>();
//        for (HasContainer hasContainer : hasContainers) {
//            if (hasContainer.getPredicate() instanceof AndP) {
//                AndP<?> andP = (AndP) hasContainer.getPredicate();
//                List<? extends P<?>> predicates = andP.getPredicates();
//                if (predicates.size() == 2) {
//                    if (predicates.get(0).getBiPredicate() == Compare.gt && predicates.get(1).getBiPredicate() == Compare.lt) {
//                        replacedStep.addHasContainer(hasContainer);
//                        result.add(hasContainer);
//                    }
//                }
//            }
//        }
//        return result;
//    }
//
//    private List<HasContainer> optimizeOutside(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
//        List<HasContainer> result = new ArrayList<>();
//        for (HasContainer hasContainer : hasContainers) {
//            if (hasContainer.getPredicate() instanceof OrP) {
//                OrP<?> orP = (OrP) hasContainer.getPredicate();
//                List<? extends P<?>> predicates = orP.getPredicates();
//                if (predicates.size() == 2) {
//                    if (predicates.get(0).getBiPredicate() == Compare.lt && predicates.get(1).getBiPredicate() == Compare.gt) {
//                        replacedStep.addHasContainer(hasContainer);
//                        result.add(hasContainer);
//                    }
//                }
//            }
//        }
//        return result;
//    }
//
//    private List<HasContainer> optimizeTextContains(ReplacedStep<?, ?> replacedStep, List<HasContainer> hasContainers) {
//        List<HasContainer> result = new ArrayList<>();
//        for (HasContainer hasContainer : hasContainers) {
//            if (hasContainer.getBiPredicate() instanceof Text ||
//                    hasContainer.getBiPredicate() instanceof FullText
//                    ) {
//                replacedStep.addHasContainer(hasContainer);
//                result.add(hasContainer);
//            }
//        }
//        return result;
//    }
//
//    private boolean isTextContains(List<HasContainer> hasContainers) {
//        return (hasContainers.size() == 1 && !hasContainers.get(0).getKey().equals(T.label.getAccessor()) &&
//                !hasContainers.get(0).getKey().equals(T.id.getAccessor()) &&
//                (hasContainers.get(0).getBiPredicate() instanceof Text ||
//                        hasContainers.get(0).getBiPredicate() instanceof FullText
//                ));
//    }
//
//    static boolean isElementValueComparator(OrderGlobalStep orderGlobalStep) {
//        return orderGlobalStep.getComparators().stream().allMatch(c -> (c instanceof ElementValueComparator
//                && (((ElementValueComparator) c).getValueComparator() == Order.incr ||
//                ((ElementValueComparator) c).getValueComparator() == Order.decr))
//                || (c instanceof Pair<?, ?>
//                && ((Pair<?, ?>) c).getValue0() instanceof ElementValueTraversal<?>
//                && ((Pair<?, ?>) c).getValue1() instanceof Order)
//        );
//    }
//
//    static boolean isTraversalComparatorWithSelectOneStep(OrderGlobalStep orderGlobalStep) {
//        for (final Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>> pair : ((ComparatorHolder<Object, Comparable>) orderGlobalStep).getComparators()) {
//            Traversal.Admin<Object, Comparable> traversal = pair.getValue0();
//            List<Step> traversalComparatorSteps = traversal.getSteps();
//            return traversalComparatorSteps.size() == 1 && traversalComparatorSteps.get(0) instanceof SelectOneStep;
//        }
//        return false;
//    }
//
//    private boolean isSingleBiPredicate(List<HasContainer> hasContainers) {
//        if (hasContainers.size() == 1) {
//            return SUPPORTED_BI_PREDICATE.contains(hasContainers.get(0).getBiPredicate());
//        } else {
//            return false;
//        }
//    }
//
//    private boolean isBetween(List<HasContainer> hasContainers) {
//        if (hasContainers.size() == 2) {
//            HasContainer hasContainer1 = hasContainers.get(0);
//            HasContainer hasContainer2 = hasContainers.get(1);
//            return hasContainer1.getBiPredicate().equals(Compare.gte) && hasContainer2.getBiPredicate().equals(Compare.lt);
//        } else {
//            return false;
//        }
//    }
//
//    private boolean isInside(List<HasContainer> hasContainers) {
//        if (hasContainers.size() == 2) {
//            HasContainer hasContainer1 = hasContainers.get(0);
//            HasContainer hasContainer2 = hasContainers.get(1);
//            return hasContainer1.getBiPredicate().equals(Compare.gt) && hasContainer2.getBiPredicate().equals(Compare.lt);
//        } else {
//            return false;
//        }
//    }
//
//    private <V> boolean isOutside(List<HasContainer> hasContainers) {
//        if (hasContainers.size() == 1 && hasContainers.get(0).getPredicate() instanceof OrP) {
//            OrP<V> orP = (OrP) hasContainers.get(0).getPredicate();
//            if (orP.getPredicates().size() == 2) {
//                P<V> predicate1 = orP.getPredicates().get(0);
//                P<V> predicate2 = orP.getPredicates().get(1);
//                return predicate1.getBiPredicate().equals(Compare.lt) && predicate2.getBiPredicate().equals(Compare.gt);
//            } else {
//                return false;
//            }
//        } else {
//            return false;
//        }
//    }
//
//    private boolean isWithinOut(List<HasContainer> hasContainers) {
//        return (hasContainers.size() == 1 && !hasContainers.get(0).getKey().equals(T.label.getAccessor()) &&
//                (hasContainers.get(0).getBiPredicate() == Contains.without || hasContainers.get(0).getBiPredicate() == Contains.within));
//    }
//
//
//    private boolean flattenChooseStep(List<Step> steps, ListIterator<Step> stepIterator, ChooseStep chooseStep, Traversal.Admin<?, ?> traversal) {
//        int stepsAdded = 0;
//        if (unoptimizableChooseStep(steps, stepIterator.previousIndex())) {
//            this.logger.debug("gremlin not optimized due to ChooseStep with ... " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
//            throw new UnoptimizableException();
//        }
//        boolean chooseStepAdded = false;
//        List<Traversal.Admin<?, ?>> localChildren = chooseStep.getLocalChildren();
//        Preconditions.checkState(localChildren.size() == 1, "ChooseStep's localChildren must have size 1, one for the predicate traversal");
//
//        List<Traversal.Admin<?, ?>> globalChildren = chooseStep.getGlobalChildren();
//        Preconditions.checkState(globalChildren.size() == 2, "ChooseStep's globalChildren must have size 2, one for true and one for false");
//        //Talk to Marko about the assumption made regarding the order of the GlobalChildren
//        Traversal.Admin<?, ?> trueTraversal;
//        Traversal.Admin<?, ?> falseTraversal;
//        Traversal.Admin<?, ?> a = globalChildren.get(0);
//        Traversal.Admin<?, ?> b = globalChildren.get(1);
//        if (a.getSteps().stream().filter(s -> s instanceof IdentityStep).findAny().isPresent()) {
//            trueTraversal = b;
//        } else {
//            trueTraversal = a;
//        }
//
//        boolean addedNestedChooseStep = false;
//        List<Step> chooseStepInternalVertexSteps = trueTraversal.getSteps();
//        for (Step internalVertexStep : chooseStepInternalVertexSteps) {
//            if (internalVertexStep instanceof ChooseStep.EndStep) {
//                break;
//            }
//            internalVertexStep.setPreviousStep(chooseStep.getPreviousStep());
//            stepIterator.add(internalVertexStep);
//            stepIterator.previous();
//            stepIterator.next();
//            stepsAdded++;
//            chooseStepAdded = true;
//            if (internalVertexStep instanceof ChooseStep) {
//                addedNestedChooseStep = true;
//            }
//        }
//        if (!addedNestedChooseStep) {
//            stepIterator.add(EmptyStep.instance());
//            stepIterator.previous();
//            stepIterator.next();
//            stepsAdded++;
//        }
//        if (traversal.getSteps().contains(chooseStep)) {
//            traversal.removeStep(chooseStep);
//        }
//        //this is needed for the stepIterator.next() to be the newly inserted steps
//        for (int i = 0; i < stepsAdded; i++) {
//            stepIterator.previous();
//        }
//        return chooseStepAdded;
//    }
//
//    private boolean flattenRepeatStep(List<Step> steps, ListIterator<Step> stepIterator, RepeatStep repeatStep, Traversal.Admin<?, ?> traversal, MutableInt repeatStepsAdded) {
//        if (unoptimizableRepeat(steps, stepIterator.previousIndex())) {
//            this.logger.debug("gremlin not optimized due to RepeatStep with emit. " + traversal.toString() + "\nPath to gremlin:\n" + ExceptionUtils.getStackTrace(new Throwable()));
//            throw new UnoptimizableException();
//        }
//        repeatStepsAdded.setValue(0);
//        boolean repeatStepAdded = false;
//        List<Traversal.Admin<?, ?>> repeatTraversals = repeatStep.getGlobalChildren();
//        Traversal.Admin admin = repeatTraversals.get(0);
//        List<Step> repeatStepInternalVertexSteps = admin.getSteps();
//        //this is guaranteed by the previous check unoptimizableRepeat(...)
//        LoopTraversal loopTraversal;
//        long numberOfLoops;
//        loopTraversal = (LoopTraversal) repeatStep.getUntilTraversal();
//        numberOfLoops = loopTraversal.getMaxLoops();
//        for (int i = 0; i < numberOfLoops; i++) {
//            for (Step internalVertexStep : repeatStepInternalVertexSteps) {
//                if (internalVertexStep instanceof RepeatStep.RepeatEndStep) {
//                    break;
//                }
//                internalVertexStep.setPreviousStep(repeatStep.getPreviousStep());
//                stepIterator.add(internalVertexStep);
//                stepIterator.previous();
//                stepIterator.next();
//                repeatStepAdded = true;
//                repeatStepsAdded.increment();
//            }
//        }
//        traversal.removeStep(repeatStep);
//        //this is needed for the stepIterator.next() to be the newly inserted steps
//        for (int i = 0; i < repeatStepsAdded.getValue(); i++) {
//            stepIterator.previous();
//        }
//        return repeatStepAdded;
//    }
//
//    private void addHasContainerForIds(SqlgGraphStepCompiled sqlgGraphStepCompiled, ReplacedStep replacedStep) {
//        Object[] ids = sqlgGraphStepCompiled.getIds();
//        List<Object> recordsIds = new ArrayList<>();
//        for (Object id : ids) {
//            if (id instanceof Element) {
//                recordsIds.add(((Element) id).id());
//            } else {
//                recordsIds.add(id);
//            }
//        }
//        HasContainer idHasContainer = new HasContainer(T.id.getAccessor(), P.within(recordsIds.toArray()));
//        replacedStep.addHasContainers(Collections.singletonList(idHasContainer));
//        sqlgGraphStepCompiled.clearIds();
//    }
//
//    private class UnoptimizableException extends RuntimeException {
//    }
}
