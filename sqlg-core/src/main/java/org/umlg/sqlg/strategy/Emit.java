package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.Optional;

/**
 * Created by pieter on 2015/10/26.
 */
public class Emit<E extends SqlgElement> {

    boolean useCurrentEmitTree;
    private Path path;
    private Pair<E, Optional<Long>> elementPlusEdgeId;
    private boolean untilFirst;
    private boolean emitFirst;

    public Emit(Pair<E, Optional<Long>> elementPlusEdgeId, boolean untilFirst, boolean emitFirst) {
        this.elementPlusEdgeId = elementPlusEdgeId;
        this.untilFirst = untilFirst;
        this.emitFirst = emitFirst;
    }

    public boolean isUseCurrentEmitTree() {
        return useCurrentEmitTree;
    }

    public void setUseCurrentEmitTree(boolean useCurrentEmitTree) {
        this.useCurrentEmitTree = useCurrentEmitTree;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public Pair<E, Optional<Long>> getElementPlusEdgeId() {
        return elementPlusEdgeId;
    }

    public boolean isUntilFirst() {
        return untilFirst;
    }

    public boolean isEmitFirst() {
        return emitFirst;
    }

    public boolean emitUntilBothStartOrEnd() {
        return emitAndUntilBothAtStart() && emitAndUntilBothAtEnd();
    }

    public boolean emitAndUntilBothAtStart() {
        return this.untilFirst && this.emitFirst;
    }

    public boolean emitAndUntilBothAtEnd() {
        return !this.untilFirst && !this.emitFirst;
    }

    @Override
    public String toString() {
        String result = "";
        if (this.path != null) {
            result += this.path.toString();
            result += ", ";
        }
//        result += elementPlusEdgeId.toString() + ", " + this.degree;
        result += elementPlusEdgeId.toString() + ", useCurrentEmitTree=" + this.useCurrentEmitTree;
        return result;
    }
}
