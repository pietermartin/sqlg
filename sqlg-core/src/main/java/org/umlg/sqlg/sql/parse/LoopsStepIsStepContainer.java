package org.umlg.sqlg.sql.parse;

import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsStep;

public record LoopsStepIsStepContainer(LoopsStep loopsStep, IsStep isStep)  {

}
