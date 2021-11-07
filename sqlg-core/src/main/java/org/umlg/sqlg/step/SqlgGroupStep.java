package org.umlg.sqlg.step;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.umlg.sqlg.step.barrier.SqlgReducingStepBarrier;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.structure.traverser.SqlgGroupByTraverser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/11/24
 */
public class SqlgGroupStep<K, V> extends SqlgReducingStepBarrier<SqlgElement, Map<K, V>> {

    private final List<String> groupBy;
    private final String aggregateOn;
    private final boolean isPropertiesStep;
    private boolean resetted = false;
    private final REDUCTION reduction;

    public enum REDUCTION {
        MIN,
        MAX,
        SUM,
        MEAN,
        COUNT
    }

    public SqlgGroupStep(Traversal.Admin traversal, List<String> groupBy, String aggregateOn, boolean isPropertiesStep, REDUCTION reduction) {
        super(traversal);
        this.groupBy = groupBy;
        this.aggregateOn = aggregateOn;
        this.isPropertiesStep = isPropertiesStep;
        this.reduction = reduction;
        setSeedSupplier(HashMap::new);
    }

    @Override
    protected Traverser.Admin<Map<K, V>> produceFinalResult(Map<K, V> result) {
        if (this.reduction == REDUCTION.MEAN) {
            for (K k : result.keySet()) {
                result.put(k, (V) ((MeanGlobalStep.MeanNumber)result.get(k)).getFinal());
            }
        }
        return new SqlgGroupByTraverser<>(result);
    }

    @Override
    public Map<K, V> reduce(Map<K, V> end, SqlgElement sqlgElement) {
        V value;
        if (this.reduction == REDUCTION.MEAN) {
            Pair<Number, Long> avgAndWeight = sqlgElement.value(this.aggregateOn);
            value = (V) new MeanGlobalStep.MeanNumber(avgAndWeight.getLeft(), avgAndWeight.getRight());
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
                K key = sqlgElement.value(this.groupBy.get(0));
                V currentValue = end.get(key);
                if (currentValue == null) {
                    end.put(key, value);
                } else {
                    switch (this.reduction) {
                        case MIN:
                            if ((Integer)value < (Integer)currentValue) {
                                end.put(key, value);
                            }
                            break;
                        case MAX:
                            if ((Integer)value > (Integer)currentValue) {
                                end.put(key, value);
                            }
                            break;
                        case SUM:
                            Long sum = (Long) currentValue + (Long) value;
                            end.put(key, (V)sum);
                            break;
                        case MEAN:
                            MeanGlobalStep.MeanNumber current = (MeanGlobalStep.MeanNumber)currentValue;
                            MeanGlobalStep.MeanNumber v = (MeanGlobalStep.MeanNumber)value;
                            MeanGlobalStep.MeanNumber mean = current.add(v);
                            end.put(key, (V)mean);
                            break;
                        case COUNT:
                            sum = (Long) currentValue + (Long) value;
                            end.put(key, (V)sum);
                    }
                }
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
        return end;
    }

    @Override
    public void reset() {
        super.reset();
        this.resetted = true;
    }

}
