package org.umlg.sqlg.step;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.traverser.SqlgGroupByTraverser;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/11/24
 */
public class SqlgGroupStep<K, V> extends SqlgAbstractStep<SqlgElement, Map<K, V>> {

    private final List<String> groupBy;
    private final String aggregateOn;
    private final boolean isPropertiesStep;
    private boolean resetted = false;
    private final boolean isMean;

    public SqlgGroupStep(Traversal.Admin traversal, List<String> groupBy, String aggregateOn, boolean isPropertiesStep, boolean isMean) {
        super(traversal);
        this.groupBy = groupBy;
        this.aggregateOn = aggregateOn;
        this.isPropertiesStep = isPropertiesStep;
        this.isMean = isMean;
    }

    @Override
    protected Traverser.Admin<Map<K, V>> processNextStart() throws NoSuchElementException {
        SqlgGroupByTraverser<K, V> end = new SqlgGroupByTraverser<>(new HashMap<>());
        while (this.starts.hasNext()) {
            if (this.resetted) {
                this.resetted = false;
                return end;
            }
            final Traverser.Admin<SqlgElement> start = this.starts.next();
            SqlgElement sqlgElement = start.get();
            V value;
            if (this.isMean) {
                Pair<Number, Long> avgAndWeight = sqlgElement.value(this.aggregateOn);
                value = (V) avgAndWeight.getLeft();
            } else {
                value = sqlgElement.value(this.aggregateOn);
            }
            if (this.groupBy.size() == 1) {
                if (this.groupBy.get(0).equals(T.label.getAccessor())) {
                    Property<?> property = sqlgElement.property(this.aggregateOn);
                    if (property.isPresent()) {
                        end.put((K) sqlgElement.label(), value);
                    } else {
                        end.put((K) sqlgElement.label(), (V) Integer.valueOf(1));
                    }
                } else {
                    end.put(sqlgElement.value(this.groupBy.get(0)), value);
                }
            } else if (this.isPropertiesStep) {
                end.put((K) IteratorUtils.list(sqlgElement.values(this.groupBy.toArray(new String[]{}))), value);
            } else {
                Map<String, List<?>> keyMap = new HashMap<>();
                for (String s : this.groupBy) {
                    List<?> keyValues = new ArrayList<>();
                    keyValues.add(sqlgElement.value(s));
                    keyMap.put(s, keyValues);
                }
                end.put((K) keyMap, value);
            }
        }
        if (end.get().isEmpty()) {
            throw FastNoSuchElementException.instance();
        }
        return end;
    }

    @Override
    public void reset() {
        this.resetted = true;
    }


}
