package org.umlg.sqlg.strategy;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.javatuples.Pair;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.*;

/**
 * Created by pieter on 2015/10/26.
 */
@SuppressWarnings("ALL")
public class Emit<E extends SqlgElement> implements Comparable<Emit<E>> {

    private Path path;
    private E element;
    private Set<String> labels;
    private boolean repeat;
    private boolean repeated;
    private boolean fake;
    //This is set to true for local optional step where the query has no labels, i.e. for a single SchemaTableTree only.
    //In this case the element will already be on the traverser i.e. the incoming element.
    private boolean incomingOnlyLocalOptionalStep;
    /**
     * This is the SqlgComparatorHolder for the SqlgElement that is being emitted.
     * It represents the {@link org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep} for the SqlgElement that is being emitted.
     */
    private SqlgComparatorHolder sqlgComparatorHolder;
    /**
     * This represents all the {@link org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep}s, one for each element along the path.
     */
    private List<SqlgComparatorHolder> sqlgComparatorHolders;

    private long parentIndex;
    private Traverser.Admin<E> traverser;
    private List<Pair<Object, Comparator<?>>> comparatorValues;

    /**
     * The {@link org.umlg.sqlg.sql.parse.ReplacedStep}'s depth
     */
    private int replacedStepDepth;

    public Emit() {
        this.fake = true;
        this.labels = Collections.emptySet();
    }

    public Emit(E element, Set<String> labels, int replacedStepDepth, SqlgComparatorHolder sqlgComparatorHolder) {
        this.element = element;
        this.labels = labels;
        this.replacedStepDepth = replacedStepDepth;
        this.sqlgComparatorHolder = sqlgComparatorHolder;
    }

    public Emit(long parentIndex, E element, Set<String> labels, int replacedStepDepth, SqlgComparatorHolder sqlgComparatorHolder) {
        this.parentIndex = parentIndex;
        this.element = element;
        this.labels = labels;
        this.replacedStepDepth = replacedStepDepth;
        this.sqlgComparatorHolder = sqlgComparatorHolder;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public E getElement() {
        return this.element;
    }

    public Set<String> getLabels() {
        return labels;
    }

    public SqlgComparatorHolder getSqlgComparatorHolder() {
        return this.sqlgComparatorHolder;
    }

    public void setSqlgComparatorHolders(List<SqlgComparatorHolder> sqlgComparatorHolders) {
        this.sqlgComparatorHolders = sqlgComparatorHolders;
    }

    public boolean isRepeat() {
        return repeat;
    }

    public void setRepeat(boolean repeat) {
        this.repeat = repeat;
    }

    public boolean isRepeated() {
        return repeated;
    }

    public void setRepeated(boolean repeated) {
        this.repeated = repeated;
    }

    public boolean isFake() {
        return fake;
    }

    public boolean isIncomingOnlyLocalOptionalStep() {
        return incomingOnlyLocalOptionalStep;
    }

    public void setIncomingOnlyLocalOptionalStep(boolean incomingOnlyLocalOptionalStep) {
        this.incomingOnlyLocalOptionalStep = incomingOnlyLocalOptionalStep;
    }

    public Traverser.Admin<E> getTraverser() {
        return traverser;
    }

    public void setTraverser(Traverser.Admin<E> traverser) {
        this.traverser = traverser;
    }

    @Override
    public String toString() {
        String result = "";
        if (!this.fake) {
            if (this.path != null) {
                result += this.path.toString();
                result += ", ";
            }
            result += element.toString();
        } else {
            result = "fake emit";
        }
        return result;
    }

    /**
     * @param pathSize Indicates the head object path size.
     *                 For SqlgVertexStepCompile they are objects that are already on the path before the step is executed.
     */
    public void evaluateElementValueTraversal(int pathSize, Traverser.Admin<E> traverser) {
        if (this.comparatorValues == null) {
            this.comparatorValues = new ArrayList<>();
        }
        //loop in reverse, from end to start.
        for (int i = this.sqlgComparatorHolders.size() - 1; i >= 0; i--) {
            SqlgElement sqlgElement;
            SqlgComparatorHolder comparatorHolder = this.sqlgComparatorHolders.get(i);
            if (comparatorHolder.hasComparators()) {
                if (comparatorHolder.hasPrecedingSelectOneLabel()) {
                    //Go get the element to compare against.
                    String precedingLabel = comparatorHolder.getPrecedingSelectOneLabel();
                    sqlgElement = traverser.path().get(precedingLabel);
                } else {
                    sqlgElement = (SqlgElement) traverser.path().objects().get(i + pathSize);
                }
                for (Pair<Traversal.Admin<?, ?>, Comparator<?>> traversalComparator : comparatorHolder.getComparators()) {
                    Traversal.Admin<?, ?> traversal = traversalComparator.getValue0();
                    Comparator comparator = traversalComparator.getValue1();

                    if (traversal.getSteps().size() == 1 && traversal.getSteps().get(0) instanceof SelectOneStep) {
                        //xxxxx.select("a").order().by(select("a").by("name"), Order.decr)
                        SelectOneStep selectOneStep = (SelectOneStep) traversal.getSteps().get(0);
                        Preconditions.checkState(selectOneStep.getScopeKeys().size() == 1, "toOrderByClause expects the selectOneStep to have one scopeKey!");
                        Preconditions.checkState(selectOneStep.getLocalChildren().size() == 1, "toOrderByClause expects the selectOneStep to have one traversal!");
                        Preconditions.checkState(
                                selectOneStep.getLocalChildren().get(0) instanceof ValueTraversal ||
                                        selectOneStep.getLocalChildren().get(0) instanceof TokenTraversal,
                                "toOrderByClause expects the selectOneStep's traversal to be a ElementValueTraversal or a TokenTraversal!");
                        String selectKey = (String) selectOneStep.getScopeKeys().iterator().next();
                        SqlgElement sqlgElementSelect = traverser.path().get(selectKey);
                        Traversal.Admin<?, ?> t = (Traversal.Admin<?, ?>) selectOneStep.getLocalChildren().get(0);
                        if (t instanceof ValueTraversal) {
                            ValueTraversal elementValueTraversal = (ValueTraversal) t;
                            this.comparatorValues.add(Pair.with(sqlgElementSelect.value(elementValueTraversal.getPropertyKey()), comparator));
                        } else {
                            TokenTraversal tokenTraversal = (TokenTraversal) t;
                            this.comparatorValues.add(Pair.with(tokenTraversal.getToken().apply(sqlgElementSelect), comparator));
                        }
                    } else if (traversal instanceof IdentityTraversal) {
                        //This is for Order.shuffle, Order.shuffle can not be used in Collections.sort(), it violates the sort contract.
                        //Basically its a crap comparator.
                        this.comparatorValues.add(Pair.with(new Random().nextInt(), Order.asc));
                    } else if (traversal instanceof ValueTraversal) {
                        ValueTraversal elementValueTraversal = (ValueTraversal) traversal;
                        this.comparatorValues.add(Pair.with(sqlgElement.value(elementValueTraversal.getPropertyKey()), comparator));
                    } else if (traversal instanceof TokenTraversal) {
                        TokenTraversal tokenTraversal = (TokenTraversal) traversal;
                        this.comparatorValues.add(Pair.with(tokenTraversal.getToken().apply(sqlgElement), comparator));
                    } else if (traversal instanceof DefaultGraphTraversal) {
                        DefaultGraphTraversal defaultGraphTraversal = (DefaultGraphTraversal) traversal;
                        final Object choice = TraversalUtil.apply(sqlgElement, defaultGraphTraversal);
                        this.comparatorValues.add(Pair.with(choice, comparator));
                    } else {
                        throw new IllegalStateException("Unhandled traversal " + traversal.getClass().getName());
                    }
                }
            }
        }
    }

    @Override
    public int compareTo(Emit<E> emit) {
        if (this.replacedStepDepth != emit.replacedStepDepth) {
            //TODO what is this for again. find out and put a comment!
            return Integer.compare(this.replacedStepDepth, emit.replacedStepDepth);
        }
        for (int i = 0; i < this.comparatorValues.size(); i++) {
            Pair<Object, Comparator<?>> comparatorPair1 = this.comparatorValues.get(i);
            Pair<Object, Comparator<?>> comparatorPair2 = emit.comparatorValues.get(i);
            Object value1 = comparatorPair1.getValue0();
            Comparator comparator1 = comparatorPair1.getValue1();
            Object value2 = comparatorPair2.getValue0();
            Comparator comparator2 = comparatorPair2.getValue1();
            Preconditions.checkState(comparator1.equals(comparator2));
            int compare = comparator1.compare(value1, value2);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    public int getReplacedStepDepth() {
        return this.replacedStepDepth;
    }

    public long getParentIndex() {
        return this.parentIndex;
    }

}
