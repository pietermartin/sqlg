package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.Optional;

/**
 * Created by pieter on 2015/10/26.
 */
public class Emit<E extends SqlgElement> {

    private int degree;
    private Path path;
    private Pair<E, Optional<Long>> elementPlusEdgeId;

    public Emit(int degree, Path path, Pair<E, Optional<Long>> elementPlusEdgeId) {
        this.degree = degree;
        this.path = path;
        this.elementPlusEdgeId = elementPlusEdgeId;
    }

    public int getDegree() {
        return degree;
    }

    public Path getPath() {
        return path;
    }

    public Pair<E, Optional<Long>> getElementPlusEdgeId() {
        return elementPlusEdgeId;
    }

    @Override
    public String toString() {
        return this.path.toString() + ", " + elementPlusEdgeId.toString() + ", " + this.degree;
    }
}
