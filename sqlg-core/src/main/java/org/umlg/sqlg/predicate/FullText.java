package org.umlg.sqlg.predicate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Full text match predicate
 * This is very postgresql oriented:
 * - configuration is whatever was used to create the index
 * - plain is to switch to plainto_tsquery (no need to use operators, etc.)
 * @author jpmoresmau
 *
 */
public class FullText implements BiPredicate<String, String> {
	private static Logger logger = LoggerFactory.getLogger(FullText.class.getName());
	/**
	 * full text configuration to use
	 */
	private String configuration;
	
	/**
	 * plain mode (no operators in query)
	 */
	private boolean plain = false;
	
	private String query;
	
	/**
	 * Build full text matching predicate (use in has(column,...))
	 * @param configuration the full text configuration to use
	 * @param value the value to search for
	 * @return the predicate
	 */
	public static P<String> fullTextMatch(String configuration,final String value){
		return fullTextMatch(configuration,false, value);
	}
	
	/**
	 * Build full text matching predicate (use in has(column,...))
	 * @param configuration the full text configuration to use
	 * @param plain should we use plain mode?
	 * @param value the value to search for
	 * @return the predicate
	 */
	public static P<String> fullTextMatch(String configuration, boolean plain, final String value){
		return new P<>(new FullText(configuration,null,plain),value);
	}
	
	/**
	 * Build full text matching predicate (use in where(...))
	 * @param configuration the full text configuration to use
	 * @param plain should we use plain mode?
	 * @param query the actual query (left hand side)
	 * @param value the value to search for
	 * @return the predicate
	 */
	public static P<String> fullTextMatch(String configuration, boolean plain, final String query, final String value){
		return new P<>(new FullText(configuration,query,plain),value);
	}
	
	/**
	 * Build full text matching predicate (use in where(...))
	 * Uses several columns for text search. This assumes PostgreSQL and concatenates column names with a space in between
	 * just like we would by default build the index
	 * @param configuration the full text configuration to use
	 * @param plain should we use plain mode?
	 * @param columns the columns to query
	 * @param value the value to search for
	 * @return the predicate
	 */
	public static P<String> fullTextMatch(String configuration, boolean plain, final List<String> columns, final String value){
		StringBuilder query=new StringBuilder(); 
		int count=1;
		for (String column : columns) {
			query.append("\""+column+"\"");
            if (count++ < columns.size()) {
            	query.append(" || ' ' || ");
            }
        }
		return new P<>(new FullText(configuration,query.toString(),plain),value);
	}
	
	/**
	 * full constructor
	 * @param configuration the full text configuration
	 * @param query the left hand side query if specified
	 * @param plain use plain mode
	 */
	public FullText(String configuration,String query,boolean plain) {
		this.configuration = configuration;
		this.query = query;
		this.plain = plain;
	}
	
	public String getConfiguration() {
		return configuration;
	}
	
	public boolean isPlain() {
		return plain;
	}
	
	public String getQuery() {
		return query;
	}
	
	@Override
	public boolean test(String first, String second) {
		logger.warn("Using Java implementation of FullText search instead of database");
		Set<String> words1=new HashSet<>(Arrays.asList(first.toLowerCase(Locale.ENGLISH).split("\\s")));
		Set<String> words2=new HashSet<>(Arrays.asList(second.toLowerCase(Locale.ENGLISH).split("\\s")));
		return words1.containsAll(words2);
	}

	@Override
	public String toString() {
		return "FullText('"+configuration+"')";
	}
}
