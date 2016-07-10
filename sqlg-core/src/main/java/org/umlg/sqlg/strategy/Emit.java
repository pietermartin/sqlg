package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.umlg.sqlg.structure.SqlgElement;

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

    public Emit() {
        this.fake = true;
    }

    public Emit(E element, Set<String> labels) {
        this.element = element;
        this.labels = labels;
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
