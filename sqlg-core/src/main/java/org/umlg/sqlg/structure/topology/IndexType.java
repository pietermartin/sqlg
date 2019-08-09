package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Specification of an index type (unique, GIN, etc).
 * Created by pieter on 2016/10/27.
 */
public class IndexType {

	// GIN index is different from GIN_FULLTEXT since it do not need any configuration property
	public static final String GIN_INDEX_NAME = "GIN";

	public static final IndexType UNIQUE=new IndexType("UNIQUE");
	public static final IndexType NON_UNIQUE=new IndexType("NON_UNIQUE");
	public static final IndexType GIN=new IndexType(GIN_INDEX_NAME);

	public static final String GIN_FULLTEXT="GIN_FULLTEXT";
	public static final String GIN_CONFIGURATION="config";
	
	public static IndexType getFullTextGIN(String configuration){
		IndexType it=new IndexType(GIN_FULLTEXT);
		it.getProperties().put(GIN_CONFIGURATION, configuration);
		return it;
	}
	
    private final String name;
    private final Map<String,String> properties=new HashMap<>();
    
	public IndexType(String name) {
		super();
		this.name = name;
	}

	public String getName() {
		return name;
	}
	
	public Map<String, String> getProperties() {
		return properties;
	}

	public boolean isGIN(){
		return GIN_INDEX_NAME.equals(name) || GIN_FULLTEXT.equals(name);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexType other = (IndexType) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (properties == null) {
            return other.properties == null;
		} else return properties.equals(other.properties);
    }

	@Override
	public String toString() {
		return toNotifyJson().toString();
	}
	 
    
	public JsonNode toNotifyJson() {
		 ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
	     result.put("name", this.name);
	     for (String k:properties.keySet()){
	    	 result.put(k, properties.get(k));
	     }
	     return result;
	}
	
	public static IndexType fromString(String node) {
		try {
			return fromNotifyJson(Topology.OBJECT_MAPPER.readTree(node));
		} catch (Exception e){
			throw new RuntimeException(e);
		}
	}
	
	public static IndexType fromNotifyJson(JsonNode node){
		// older version
		if(node.isValueNode()){
			String s=node.asText();
			if ("UNIQUE".equalsIgnoreCase(s)){
				return UNIQUE;
			} else if ("NON_UNIQUE".equalsIgnoreCase(s)){
				return NON_UNIQUE;
			}
		}
		
		IndexType it=new IndexType(node.get("name").asText());
		Iterator<String> keys=node.fieldNames();
		while (keys.hasNext()){
			String k=keys.next();
			if (!"name".equals(k)){
				it.getProperties().put(k, node.get(k).asText());
			}
		}
		return it;
	}
}
