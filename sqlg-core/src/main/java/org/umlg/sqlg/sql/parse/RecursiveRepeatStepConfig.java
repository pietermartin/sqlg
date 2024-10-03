package org.umlg.sqlg.sql.parse;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public record RecursiveRepeatStepConfig(Direction direction, String edge, boolean includeEdge, ReplacedStepTree<Vertex, Vertex> untilReplacedStepTree, boolean hasNotStepForPathTraversal, boolean hasLoopAndIsStep) {
}
