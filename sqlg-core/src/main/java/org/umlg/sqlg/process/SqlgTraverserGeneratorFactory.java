package org.umlg.sqlg.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserGeneratorFactory;

/**
 * Created by pieter on 2015/07/20.
 */
public class SqlgTraverserGeneratorFactory implements TraverserGeneratorFactory {

    @Override
    public TraverserGenerator getTraverserGenerator(Traversal.Admin<?, ?> traversal) {
        return SqlgGraphStepWithPathTraverserGenerator.instance();
    }
}
