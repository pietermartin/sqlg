package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.umlg.sqlg.structure.SqlgElement;

/**
 * Created by pieter on 2015/10/26.
 */
public class Emit<E extends SqlgElement> {

    boolean useCurrentEmitTree;
    boolean useOptionalTree;
    private Path path;
    private E element;

    public Emit(E element, boolean useOptionalTree) {
        this.element = element;
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

    public E getElement() {
        return element;
    }

    @Override
    public String toString() {
        String result = "";
        if (this.path != null) {
            result += this.path.toString();
            result += ", ";
        }
        result += element.toString() + ", useCurrentEmitTree=" + this.useCurrentEmitTree;
        return result;
    }
}
