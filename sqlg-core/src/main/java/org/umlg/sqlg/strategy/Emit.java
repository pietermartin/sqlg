package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.List;
import java.util.Set;

/**
 * Created by pieter on 2015/10/26.
 */
public class Emit<E extends SqlgElement> {

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

    public Emit() {
        this.fake = true;
    }

    //    public Emit(E element, Set<String> labels, List<Pair<Traversal.Admin, Comparator>> comparators) {
    public Emit(E element, Set<String> labels, SqlgComparatorHolder sqlgComparatorHolder) {
        this.element = element;
        this.labels = labels;
        this.sqlgComparatorHolder = sqlgComparatorHolder;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public E getElement() {
        return element;
    }

    public Set<String> getLabels() {
        return labels;
    }

    public SqlgComparatorHolder getSqlgComparatorHolder() {
        return sqlgComparatorHolder;
    }

    public List<SqlgComparatorHolder> getSqlgComparatorHolders() {
        return sqlgComparatorHolders;
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

    boolean isIncomingOnlyLocalOptionalStep() {
        return incomingOnlyLocalOptionalStep;
    }

    public void setIncomingOnlyLocalOptionalStep(boolean incomingOnlyLocalOptionalStep) {
        this.incomingOnlyLocalOptionalStep = incomingOnlyLocalOptionalStep;
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

}
