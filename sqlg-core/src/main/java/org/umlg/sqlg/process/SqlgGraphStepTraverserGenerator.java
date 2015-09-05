package org.umlg.sqlg.process;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Created by pieter on 2015/07/20.
 */
public class SqlgGraphStepTraverserGenerator implements TraverserGenerator {

    private static final SqlgGraphStepTraverserGenerator INSTANCE = new SqlgGraphStepTraverserGenerator();
    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.OBJECT,
            TraverserRequirement.BULK,
            TraverserRequirement.SINGLE_LOOP,
            TraverserRequirement.NESTED_LOOP,
            TraverserRequirement.SACK,
            TraverserRequirement.SIDE_EFFECTS);

    private SqlgGraphStepTraverserGenerator() {
    }

    @Override
    public <S> Traverser.Admin<S> generate(final S pair, final Step<S, ?> step, final long initialBulk) {
        //This sucks, ReducingBarrierStep call the generator again
        if (pair instanceof Pair) {
            Pair<S, Multimap<String, Object>> p = (Pair<S, Multimap<String, Object>>) pair;
            return new SqlGraphStepTraverser<>(p.getLeft(), p.getRight(), step, initialBulk);
        } else {
            Multimap<String, Object> emptyMap = ArrayListMultimap.create();
            return new SqlGraphStepTraverser<>(pair, emptyMap, step, initialBulk);
        }
    }

    @Override
    public Set<TraverserRequirement> getProvidedRequirements() {
        return REQUIREMENTS;
    }

    public static SqlgGraphStepTraverserGenerator instance() {
        return INSTANCE;
    }
}
