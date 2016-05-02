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
    boolean useOptionalTree;
    private Path path;
    private Pair<E, Optional<Long>> elementPlusEdgeId;
    private boolean untilFirst;
    private boolean emitFirst;

    public Emit(Pair<E, Optional<Long>> elementPlusEdgeId, boolean untilFirst, boolean emitFirst, boolean useOptionalTree/*, int numberOfSteps*/) {
        this.elementPlusEdgeId = elementPlusEdgeId;
        this.untilFirst = untilFirst;
        this.emitFirst = emitFirst;
        this.useOptionalTree = useOptionalTree;
    }

    public boolean isUseCurrentEmitTree() {
        return useCurrentEmitTree;
    }

    public void setUseCurrentEmitTree(boolean useCurrentEmitTree) {
        this.useCurrentEmitTree = useCurrentEmitTree;
    }

    public boolean isUseOptionalTree() {
        return useOptionalTree;
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

    @Override
    public String toString() {
        String result = "";
        if (this.path != null) {
            result += this.path.toString();
            result += ", ";
        }
        result += elementPlusEdgeId.toString() + ", useCurrentEmitTree=" + this.useCurrentEmitTree;
        return result;
    }
}
