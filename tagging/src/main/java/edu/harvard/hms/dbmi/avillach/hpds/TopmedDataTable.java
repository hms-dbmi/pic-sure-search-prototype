package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.jsoup.nodes.Document;

import com.google.common.collect.Sets;

import edu.harvard.hms.dbmi.avillach.hpds.model.SearchQuery;

public class TopmedDataTable implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2138670854234447527L;
	public SortedMap<String, String> metadata;
	public SortedMap<String, TopmedVariable> variables;
	/**
	 * This field is enormous and should not be serialized
	 */
	@JsonIgnore
	public Map<String, Set<TopmedVariable>> tagMap;

	public TopmedDataTable(){
		variables = new TreeMap<String, TopmedVariable>();
		metadata = new TreeMap<String, String>();
		tagMap = new HashMap<String, Set<TopmedVariable>>();
	}

	public Map<Double, List<TopmedVariable>> searchVariables(SearchQuery searchQuery) {
		Set<TopmedVariable> variablesInScope = new HashSet<TopmedVariable>(variables.values());
		String input = searchQuery.getSearchTerm();
		List<String> requiredTags = searchQuery.getIncludedTags();
		List<String> excludedTags = searchQuery.getExcludedTags();
		HashMap<String, Set<TopmedVariable>> variablesPerTag = new HashMap<>();
		for(String tag : requiredTags) {
			Set<TopmedVariable> variablesForTag = tagMap.get(tag);
			if(variablesForTag!=null) {
				variablesPerTag.put(tag, variablesForTag);
			}else {
				return new HashMap<Double, List<TopmedVariable>>();
			}
		}
		for(Set<TopmedVariable> variables : variablesPerTag.values()) {
			variablesInScope = Sets.intersection(variablesInScope, variables);
		}
		if(excludedTags!=null) {
			for(String tag : excludedTags) {
				Set<TopmedVariable> variablesForTag = tagMap.get(tag);
				if(variablesForTag!=null) {
					variablesInScope = Sets.difference(variablesInScope, variablesForTag);
				}
			}
		}

		Map<Double, List<TopmedVariable>> results = variablesInScope.stream().collect(
						Collectors.groupingBy((variable)->{
							return variable.relevance(input);
						},Collectors.toList()));
		results.remove(0d);
		return results;
	}

}
