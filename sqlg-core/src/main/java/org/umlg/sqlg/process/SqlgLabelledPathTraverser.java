package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Path;

/**
 * Created by pieter on 2015/07/26.
 */
public interface SqlgLabelledPathTraverser {

    void setPath(Path path);
    Path getPath();
}
