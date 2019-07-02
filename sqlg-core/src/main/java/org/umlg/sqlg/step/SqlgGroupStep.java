package org.umlg.sqlg.step;

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

    private List<String> groupBy;
    private String aggregateOn;
    private boolean isPropertiesStep;
    private boolean resetted = false;

    public SqlgGroupStep(Traversal.Admin traversal, List<String> groupBy, String aggregateOn, boolean isPropertiesStep) {
        super(traversal);
        this.groupBy = groupBy;
        this.aggregateOn = aggregateOn;
        this.isPropertiesStep = isPropertiesStep;
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
            if (this.groupBy.size() == 1) {
                if (this.groupBy.get(0).equals(T.label.getAccessor())) {
                    Property property = sqlgElement.property(this.aggregateOn);
                    if (property.isPresent()) {
                        end.put((K) sqlgElement.label(), sqlgElement.value(this.aggregateOn));
                    } else {
                        end.put((K) sqlgElement.label(), (V) Integer.valueOf(1));
                    }
                } else {
                    end.put(sqlgElement.value(this.groupBy.get(0)), sqlgElement.value(this.aggregateOn));
                }
            } else if (this.isPropertiesStep) {
                end.put((K) IteratorUtils.list(sqlgElement.values(this.groupBy.toArray(new String[]{}))), sqlgElement.value(this.aggregateOn));
            } else {
                Map<String, List<?>> keyMap = new HashMap<>();
                for (String s : this.groupBy) {
                    List<?> keyValues = new ArrayList<>();
                    keyValues.add(sqlgElement.value(s));
                    keyMap.put(s, keyValues);
                }
                end.put((K) keyMap, sqlgElement.value(this.aggregateOn));
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
