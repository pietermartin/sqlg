package org.umlg.sqlg.structure;

import org.umlg.sqlg.structure.topology.IndexType;

import java.util.ArrayList;
import java.util.List;

/**
 * a simple index reference
 * @author jpmoresmau
 *
 */
public class IndexRef {
	private final String indexName;
	private final IndexType indexType;
	private final List<String> columns;
	
	public IndexRef(String indexName, IndexType indexType, List<String> columns) {
		super();
		this.indexName = indexName;
		this.indexType = indexType;
		// clone
		this.columns=new ArrayList<>(columns);
	}
	
	public String getIndexName() {
		return indexName;
	}
	public IndexType getIndexType() {
		return indexType;
	}
	public List<String> getColumns() {
		return columns;
	}
	
	
}