package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

public class SqlgFoldStep<S, E> extends SqlgReducingStepBarrier<S, E> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT);
    private final boolean listFold;
    private final BinaryOperator<E> reducingBiOperator;

    public SqlgFoldStep(final Traversal.Admin traversal, final Supplier<E> seed, boolean listFold, BinaryOperator<E> reducingBiOperator) {
        super(traversal);
        this.listFold = listFold;
        this.setSeedSupplier(seed);
        this.reducingBiOperator = reducingBiOperator;
    }

    @Override
    public E reduce(E a, S b) {
        if (!this.listFold) {
            return this.reducingBiOperator.apply(a, (E)b);
        } else {
            if (a instanceof  List) {
                List<S> result = (List)a;
                result.add(b);
                return (E) result;
            } else {
                List<S> result = new ArrayList<>();
                result.add(b);
                return (E) result;
            }
        }
    }

//    @Override
//    public E projectTraverser(final Traverser.Admin<S> traverser) {
//        if (this.listFold) {
//            final List<S> list = new ArrayList<>();
//            for (long i = 0; i < traverser.bulk(); i++) {
//                list.add(traverser.get());
//            }
//            return (E) list;
//        } else {
//            return (E) traverser.get();
//        }
//    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    public boolean isListFold() {
        return listFold;
    }

    public static class FoldBiOperator<E> implements BinaryOperator<E>, Serializable {

        private BiFunction biFunction;

        private FoldBiOperator() {
            // for serialization purposes
        }

        public FoldBiOperator(final BiFunction biFunction) {
            this.biFunction = biFunction;
        }

        @Override
        public E apply(E seed, E other) {
            return (E) this.biFunction.apply(seed, other);
        }

    }
}
