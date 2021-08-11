package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.jsoup.nodes.Document;

import com.google.common.collect.Sets;

import edu.harvard.hms.dbmi.avillach.hpds.model.SearchQuery;

public class TopmedDataTable implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2138670854234447527L;
	public TreeMap<String, String> metadata;
	public TreeMap<String, TopmedVariable> variables;
	private HashMap<String, Set<TopmedVariable>> tagMap;

	public TopmedDataTable(){

	}

	public TopmedDataTable(Document doc, String data_dict_file){
		metadata = new TreeMap<>();
		metadata.put("id", getDataTableAttribute(doc, "id"));
		metadata.put("name", data_dict_file.split("\\.")[4]);
		metadata.put("study_id", getDataTableAttribute(doc, "study_id"));
		metadata.put("participant_set", getDataTableAttribute(doc, "participant_set"));
		metadata.put("date_created", getDataTableAttribute(doc, "date_created"));
		metadata.put("description", doc.getElementsByTag("data_table").first().getElementsByTag("description").first().text());
		variables = new TreeMap<>();
		doc.getElementsByTag("variable").stream().forEach(variable -> {
			variables.put(variable.attr("id"), new TopmedVariable(this, variable));
		});
		generateTagMap();
	}

	public void generateTagMap() {
		tagMap = new HashMap<>();
		Set<String> tags = new HashSet<String>();
		for(TopmedVariable variable : variables.values()) {
			tags.addAll(variable.getMetadata_tags());
			tags.addAll(variable.getValue_tags());
		}
		for(String tag : tags) {
			tagMap.put(tag, new HashSet<TopmedVariable>());
		}
		for(TopmedVariable variable : variables.values()) {
			for(String tag : variable.getMetadata_tags()) {
				tagMap.get(tag).add(variable);
			}
			for(String tag : variable.getValue_tags()) {
				tagMap.get(tag).add(variable);
			}
		}
	}

	private String getDataTableAttribute(Document doc, String attrName) {
		return doc.getElementsByTag("data_table").first().attr(attrName);
	}

		public Map<Double,List<TopmedVariable>> search(String input) {
			return searchVariables(SearchQuery.builder().searchTerm(input).build());
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

	public void loadVarReport(Document doc) {
		this.metadata.put("study_description", getDataTableAttribute(doc, "study_name"));
		doc.getElementsByTag("variable").stream().forEach(variable -> {
			TopmedVariable var = variables.get(variable.attr("id").replaceFirst("\\.p\\d.*$", ""));
			if(var!=null) {
				var.addVarReportMeta(variable);
			}
		});
	}
}
