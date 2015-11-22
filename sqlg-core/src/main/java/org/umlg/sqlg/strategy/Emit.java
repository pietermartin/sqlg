package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.Optional;

/**
 * Created by pieter on 2015/10/26.
 */
public class Emit<E extends SqlgElement> {

    private int degree = -1;
    private Path path;
    private Pair<E, Optional<Long>> elementPlusEdgeId;
    private boolean untilFirst;
    private boolean emitFirst;

    public Emit(Pair<E, Optional<Long>> elementPlusEdgeId, boolean untilFirst, boolean emitFirst) {
        this.elementPlusEdgeId = elementPlusEdgeId;
        this.untilFirst = untilFirst;
        this.emitFirst = emitFirst;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
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
        return this.untilFirst && this.emitFirst || !this.untilFirst && !this.emitFirst;
    }

    @Override
    public String toString() {
        String result = "";
        if (this.path != null) {
            result += this.path.toString();
            result += ", ";
        }
        result += elementPlusEdgeId.toString() + ", " + this.degree;
        return result;
    }
}
