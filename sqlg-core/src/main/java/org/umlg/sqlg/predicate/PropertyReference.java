package org.umlg.sqlg.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;

/**
 * A property reference value with a corresponding predicate, to do filtering by comparing values of columns for a given row
 * @author JP Moresmau
 *
 */
public class PropertyReference  {
	/**
	 * the column name
	 */
	private final String columnName;
	
	private static class RefP extends P<Object> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7381678193034988584L;

		private RefP(Compare biPredicate, PropertyReference value) {
			super(biPredicate, value);
		}
	}
	
	private PropertyReference(String columnName) {
		super();
		this.columnName = columnName;
	}

	/**
	 * build a predicate from a compare operation and a column name
	 * @param p
	 * @param columnName
	 * @return
	 */
	public static P<Object> propertyRef(Compare p,String columnName){
		return new RefP(p, new PropertyReference(columnName));
	}
	
	/**
	 * get the column name
	 * @return
	 */
	public String getColumnName() {
		return columnName;
	}
	
}
