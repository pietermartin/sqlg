package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgElement;

import java.io.Serializable;
import java.util.function.BiPredicate;

/**
 * Date: 2014/08/15
 * Time: 8:25 PM
 */
public class SqlgHasContainer implements Serializable {

    private String key;
    private BiPredicate predicate;
    private Object value;

    public SqlgHasContainer(HasContainer hasContainer) {
        this.key = hasContainer.key;
        this.predicate = hasContainer.predicate;
        this.value = hasContainer.value;
    }

    public boolean test(final Element element) {
        if (null != this.value) {
            if (this.key.equals(T.id.getAccessor()))
                return this.predicate.test(element.id(), this.value);
            else if (this.key.equals(T.label.getAccessor()))
                if (this.predicate == Compare.eq) {
                    SqlgElement sqlgElement = (SqlgElement)element;
                    SchemaTable labelSchemaTable = SchemaTable.of(sqlgElement.getSchema(), sqlgElement.getTable());
                    String[] schemaTableValue = ((String)this.value).split("\\.");
                    if (schemaTableValue.length==1) {
                        return element.label().endsWith((String)this.value);
                    } else if (schemaTableValue.length==2) {
                        return schemaTableValue[0].equals(labelSchemaTable.getSchema()) &&
                                schemaTableValue[1].equals(labelSchemaTable.getTable());
                    } else {
                        throw new IllegalStateException("label may only have one dot separator!");
                    }
                } else {
                    return this.predicate.test(element.label(), this.value);
                }
            else {
                final Property property = element.property(this.key);
                return property.isPresent() && this.predicate.test(property.value(), this.value);
            }
        } else {
            return Contains.within.equals(this.predicate) ?
                    element.property(this.key).isPresent() :
                    !element.property(this.key).isPresent();
        }
    }
}
