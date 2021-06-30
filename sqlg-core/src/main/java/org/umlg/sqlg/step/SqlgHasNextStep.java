package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;

import java.util.NoSuchElementException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2020/09/13
 */
public class SqlgHasNextStep<S> extends SqlgAbstractStep<S, Boolean> {

//    private long startCount;
//    private long count = 0;

    public SqlgHasNextStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<Boolean> processNextStart() throws NoSuchElementException {
//        if (count++ < startCount) {
            if (this.starts.hasNext()) {
                Traverser.Admin<S> s = this.starts.next();
                return s.split(Boolean.TRUE, this);
            } else {
                return SqlgTraverserGenerator.instance().generate(Boolean.FALSE, (Step<Boolean, ?>) this, 1L, false, false);
            }
//        } else {
//            throw FastNoSuchElementException.instance();
//        }
    }

//    public void setStartCount(long startCount) {
//        this.startCount = startCount;
//    }

}
