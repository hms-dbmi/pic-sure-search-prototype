package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.Serializable;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.jsoup.nodes.Document;

public class TopmedDataTable implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2138670854234447527L;
	public TreeMap<String, String> metadata;
	public TreeMap<String, TopmedVariable> variables;

	public TopmedDataTable(){

	}

	public TopmedDataTable(Document doc){
		metadata = new TreeMap<>();
		metadata.put("id", getDataTableAttribute(doc, "id"));
		metadata.put("study_id", getDataTableAttribute(doc, "study_id"));
		metadata.put("participant_set", getDataTableAttribute(doc, "participant_set"));
		metadata.put("date_created", getDataTableAttribute(doc, "date_created"));
		metadata.put("description", doc.getElementsByTag("data_table").first().getElementsByTag("description").first().text());
		variables = new TreeMap<>();
		doc.getElementsByTag("variable").stream().forEach(variable -> {
			variables.put(variable.attr("id"), new TopmedVariable(this, variable));
		});

	}

	private String getDataTableAttribute(Document doc, String attrName) {
		return doc.getElementsByTag("data_table").first().attr(attrName);
	}

	public Map<Double,List<TopmedVariable>> search(String input) {
		Map<Double, List<TopmedVariable>> relevantVars = variables.values().parallelStream()
				.filter((variable)->{
					return variable.relevance(input)>=1;
				}).collect(
						Collectors.groupingBy((variable)->{
							return variable.relevance(input);
						},Collectors.toList()));
		return relevantVars;
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
