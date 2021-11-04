package org.umlg.sqlg.step.barrier;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.step.SqlgFilterStep;
import org.umlg.sqlg.step.SqlgMapStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverser;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/09/28
 */
@SuppressWarnings("unchecked")
public class SqlgWhereTraversalStepBarrier<S> extends SqlgAbstractStep<S, S> implements TraversalParent, Scoping, PathProcessor {

    private Multimap<String, Traverser.Admin<S>> startRecordIds = LinkedListMultimap.create();
    private List<Traverser.Admin<S>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<S>> resultIterator;
    private boolean first = true;
    private Traversal.Admin<?, ?> whereTraversal;
    private final Set<String> scopeKeys;
    private Set<String> keepLabels;
    private SqlgWhereEndStep sqlgWhereEndStep;

    public SqlgWhereTraversalStepBarrier(final Traversal.Admin traversal, WhereTraversalStep whereTraversalStep) {
        super(traversal);
        this.whereTraversal = (Traversal.Admin<?, ?>) whereTraversalStep.getLocalChildren().get(0);
        this.scopeKeys = whereTraversalStep.getScopeKeys();

        if (this.whereTraversal.getSteps().get(0) instanceof ConnectiveStep) {
            ConnectiveStep<S> connectiveStep = (ConnectiveStep) this.whereTraversal.getSteps().get(0);
            for (Traversal.Admin<S, ?> connectiveTraversal : connectiveStep.getLocalChildren()) {
                //Replace the WhereStartStep
                List<WhereTraversalStep.WhereStartStep> whereStartSteps = TraversalHelper.getStepsOfAssignableClass(WhereTraversalStep.WhereStartStep.class, connectiveTraversal);
                for (WhereTraversalStep.WhereStartStep whereStartStep : whereStartSteps) {
                    SqlgWhereStartStep sqlgWhereStartStep = new SqlgWhereStartStep(connectiveTraversal, (String) whereStartStep.getScopeKeys().iterator().next());
                    TraversalHelper.replaceStep(whereStartStep, sqlgWhereStartStep, connectiveTraversal);

                    //Remove the WhereEndStep
                    List<WhereTraversalStep.WhereEndStep> whereEndSteps = TraversalHelper.getStepsOfAssignableClass(WhereTraversalStep.WhereEndStep.class, connectiveTraversal);
                    if (!whereEndSteps.isEmpty()) {
                        Preconditions.checkState(whereEndSteps.size() == 1);
                        WhereTraversalStep.WhereEndStep whereEndStep = whereEndSteps.get(0);
//                        this.endMatchKey = whereEndStep.getScopeKeys().iterator().next();
                        whereStartStep.getTraversal().removeStep(whereEndStep);
                    }
                }
            }
        } else {
            //Replace the WhereStartStep
            List<WhereTraversalStep.WhereStartStep> whereStartSteps = TraversalHelper.getStepsOfAssignableClassRecursively(WhereTraversalStep.WhereStartStep.class, this.whereTraversal);
            for (WhereTraversalStep.WhereStartStep whereStartStep : whereStartSteps) {
                SqlgWhereStartStep sqlgWhereStartStep;
                if (!whereStartStep.getScopeKeys().isEmpty()) {
                    sqlgWhereStartStep = new SqlgWhereStartStep(whereStartStep.getTraversal(), (String) whereStartStep.getScopeKeys().iterator().next());
                } else {
                    sqlgWhereStartStep = new SqlgWhereStartStep(whereStartStep.getTraversal(), null);
                }
                TraversalHelper.replaceStep(whereStartStep, sqlgWhereStartStep, whereStartStep.getTraversal());

                //Remove the WhereEndStep
                List<WhereTraversalStep.WhereEndStep> whereEndSteps = TraversalHelper.getStepsOfAssignableClass(WhereTraversalStep.WhereEndStep.class, whereStartStep.getTraversal());
                if (!whereEndSteps.isEmpty()) {
                    Preconditions.checkState(whereEndSteps.size() == 1);
                    WhereTraversalStep.WhereEndStep whereEndStep = whereEndSteps.get(0);
                    this.sqlgWhereEndStep = new SqlgWhereEndStep(whereStartStep.getTraversal(), whereEndStep.getScopeKeys().iterator().next());
                    TraversalHelper.replaceStep(whereEndStep, this.sqlgWhereEndStep, whereStartStep.getTraversal());
                }
            }
        }
    }

    @Override
    public ElementRequirement getMaxRequirement() {
        return TraversalHelper.getVariableLocations(this.whereTraversal).contains(Scoping.Variable.START) ?
                PathProcessor.super.getMaxRequirement() :
                ElementRequirement.ID;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
            while (this.starts.hasNext()) {
                Traverser.Admin start = this.starts.next();
                //Well now, can't say I really really get this.
                //The TinkerPop's TraversalFilterStep resets the traversal for each start.
                //This in turn clears TraverserSet's internal map. So the result for every start is added to an empty map. This means
                //no merging happens which means no bulking happens. Look at TraverserSet.add
                //This requirement says no bulking must happen.
                //If the strategy finds TraverserRequirement.ONE_BULK it will set requiredOneBulk = true on the traverser.
                //However the traverser might have been created from a step and strategy that does not care for TraverserRequirement.ONE_BULK so we force it here.
                if (start instanceof SqlgTraverser) {
                    ((SqlgTraverser) start).setRequiresOneBulk(true);
                }
                List<Object> startObjects = start.path().objects();
                StringBuilder recordIdConcatenated = new StringBuilder();
                for (Object startObject : startObjects) {
                    if (startObject instanceof Element) {
                        Element e = (Element) startObject;
                        recordIdConcatenated.append(e.id().toString());
                    } else {
                        recordIdConcatenated.append(startObject.toString());
                    }
                }
                this.startRecordIds.put(recordIdConcatenated.toString(), start);
                this.whereTraversal.addStart(start);
            }
            while (this.whereTraversal.hasNext()) {
                Traverser.Admin<?> filterTraverser = this.whereTraversal.nextTraverser();
                List<Object> filterTraverserObjects = filterTraverser.path().objects();
                String startId = "";
                for (Object filteredTraverserObject : filterTraverserObjects) {
                    if (filteredTraverserObject instanceof Element) {
                        Element e = (Element) filteredTraverserObject;
                        startId += e.id().toString();
                    } else {
                        startId += filteredTraverserObject.toString();
                    }
                    if (startRecordIds.containsKey(startId)) {
                        this.results.addAll(startRecordIds.get(startId));
                        startRecordIds.removeAll(startId);
                    }
                    if (startRecordIds.isEmpty()) {
                        break;
                    }
                }

            }
            this.results.sort((o1, o2) -> {
                SqlgTraverser x = (SqlgTraverser) o1;
                SqlgTraverser y = (SqlgTraverser) o2;
                return Long.compare(x.getStartElementIndex(), y.getStartElementIndex());
            });
            this.resultIterator = this.results.iterator();
        }
        if (this.resultIterator.hasNext()) {
            Traverser.Admin<S> traverser = this.resultIterator.next();
            return PathProcessor.processTraverserPathLabels(traverser, this.keepLabels);
        } else {
            //The standard TraversalFilterStep.filter calls TraversalUtil.test which normally resets the traversal for every incoming start.
            reset();
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        return null == this.whereTraversal ? Collections.emptyList() : Collections.singletonList(this.whereTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.whereTraversal);
    }

    @Override
    public Set<String> getScopeKeys() {
        return Collections.unmodifiableSet(this.scopeKeys);
    }

    @Override
    public SqlgWhereTraversalStepBarrier<S> clone() {
        final SqlgWhereTraversalStepBarrier<S> clone = (SqlgWhereTraversalStepBarrier<S>) super.clone();
        clone.whereTraversal = this.whereTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.whereTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.whereTraversal.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = keepLabels;
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    @Override
    public void reset() {
        super.reset();
        this.startRecordIds = LinkedListMultimap.create();
        this.results = new ArrayList<>();
        this.resultIterator = null;
        this.first = true;
        this.whereTraversal.reset();
        if (this.sqlgWhereEndStep != null) {
            this.sqlgWhereEndStep.reset();
        }
    }
    ////////////

    public class SqlgWhereStartStep<S> extends SqlgMapStep<S, Object> implements Scoping {

        private String selectKey;

        SqlgWhereStartStep(final Traversal.Admin traversal, final String selectKey) {
            super(traversal);
            this.selectKey = selectKey;
        }

        @Override
        protected Object map(final Traverser.Admin<S> traverser) {
            if (this.getTraversal().getEndStep() instanceof SqlgWhereTraversalStepBarrier.SqlgWhereEndStep)
                ((SqlgWhereEndStep) this.getTraversal().getEndStep()).processStartTraverser(traverser);
            else if (this.getTraversal().getEndStep() instanceof ProfileStep && this.getTraversal().getEndStep().getPreviousStep() instanceof SqlgWhereTraversalStepBarrier.SqlgWhereEndStep)     // TOTAL SUCKY HACK!
                ((SqlgWhereEndStep) this.getTraversal().getEndStep().getPreviousStep()).processStartTraverser(traverser);

            if (null == this.selectKey) {
                return traverser.get();
            } else {
                try {
                    return this.getScopeValue(Pop.last, this.selectKey, traverser);
                } catch (KeyNotFoundException e) {
                    return EmptyTraverser.instance();
                }
            }
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this, this.selectKey);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ (null == this.selectKey ? "null".hashCode() : this.selectKey.hashCode());
        }

        public void removeScopeKey() {
            this.selectKey = null;
        }

        @Override
        public Set<String> getScopeKeys() {
            return null == this.selectKey ? Collections.emptySet() : Collections.singleton(this.selectKey);
        }

    }

    public class SqlgWhereEndStep extends SqlgFilterStep<Object> implements Scoping {

        private final String matchKey;
        private Object matchValue = null;
        private final Map<Traverser.Admin<?>, Object> startValueMap = new HashMap<>();

        SqlgWhereEndStep(final Traversal.Admin traversal, final String matchKey) {
            super(traversal);
            this.matchKey = matchKey;
        }

        void processStartTraverser(final Traverser.Admin traverser) {
            if (null != this.matchKey) {
                this.matchValue = this.getSafeScopeValue(Pop.last, this.matchKey, traverser);
                this.startValueMap.put(traverser, this.matchValue);
            }
        }

        @Override
        protected boolean filter(final Traverser.Admin<Object> traverser) {
            if (this.matchKey == null) {
                return true;
            } else {
                List<Object> filterTraverserObjects = traverser.path().objects();
                String startId = "";
                for (Object filteredTraverserObject : filterTraverserObjects) {
                    if (filteredTraverserObject instanceof Element) {
                        Element e = (Element) filteredTraverserObject;
                        startId += e.id().toString();
                    } else {
                        startId += filteredTraverserObject.toString();
                    }
                    if (SqlgWhereTraversalStepBarrier.this.startRecordIds.containsKey(startId)) {
                        Collection<Traverser.Admin<S>> starts = SqlgWhereTraversalStepBarrier.this.startRecordIds.get(startId);
                        for (Traverser.Admin<S> start : starts) {
                            if (traverser.get().equals(this.startValueMap.get(start))) {
                                return true;
                            }

                        }
                    }
                }
                return false;
            }
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this, this.matchKey);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ (null == this.matchKey ? "null".hashCode() : this.matchKey.hashCode());
        }

        @Override
        public Set<String> getScopeKeys() {
            return null == this.matchKey ? Collections.emptySet() : Collections.singleton(this.matchKey);
        }

        @Override
        public void reset() {
            this.startValueMap.clear();
        }
    }
    ////////////

}
