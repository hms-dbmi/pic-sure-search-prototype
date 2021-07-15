package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.jsoup.nodes.Element;

public class TopmedVariable implements Serializable  {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1802723843452332984L;
	private static final List<String> EXCLUDED_WORDS_LIST = List.of(
			"IF",
			"IS",
			"FOR",
			"THE",
			"NOT",
			"OF",
			"ON",
			"OR",
			"DID",
			"BEEN",
			"SEE",
			"FROM",
			"LAST",
			"EVER",
			"WERE",
			"WHERE",
			"TO",
			"BETWEEN",
			"IN",
			"ONLY",
			"THAT",
			"AN",
			"AT",
			"YOU",
			"ANY",
			"PAST",
			"BY",
			"HAVE",
			"WHILE",
			"HAVING",
			"YOUR",
			"HAD",
			"HAS",
			"OTHER",
			"THIS",
			"THEN",
			"WAS",
			"ARE",
			"AND",
			"UNKNOWN"
			);
	private HashMap<String, String> metadata;
	private HashMap<String, String> values;
	private HashSet<String> metadata_tags = new HashSet<>();
	private HashSet<String> value_tags = new HashSet<>();
	private String dtId;
	private String studyId;

	public TopmedVariable(){
		
	}
	
	public TopmedVariable(TopmedDataTable topmedDataTable, Element e){
		this.metadata = new HashMap<>();
		this.values = new HashMap<>();
		e.getAllElements().stream().forEach((element)->{
			if(element.tag().getName().equalsIgnoreCase("value")) {
				this.values.put(element.attr("id"), element.ownText());
			} else {
				if(!element.tag().getName().equalsIgnoreCase("comment")) {
					this.metadata.put(element.tagName(), element.ownText());
				}
			}
		});
		this.dtId = topmedDataTable.metadata.get("id");
		this.studyId = topmedDataTable.metadata.get("study_id");
		this.metadata.put("varId",e.id());
		buildTags();
	}

	private void buildTags() {
		for(String value : metadata.values()) {
			metadata_tags.addAll(filterTags(value));
		}
		for(String value : values.values()) {
			value_tags.addAll(filterTags(value));
		}
		metadata_tags.add(dtId);
		metadata.put("study_id", studyId);
		metadata_tags.add(studyId);
	}

	private List<String> filterTags(String value) {
		return Arrays.asList(value.split("[\\s\\p{Punct}]"))
				.stream().filter((val2)->{
					return val2.length() > 1 
						&& !val2.matches("^\\d+$") 
						&& !EXCLUDED_WORDS_LIST.contains(val2.toUpperCase()) 
						&& !val2.toUpperCase().matches("V\\d+");}).map((String var)->{
							return var.toUpperCase();}).collect(Collectors.toList());
	}

	String lastInput = "";
	double lastScore;
	
	double relevance(String input) {
		double[] score = {0};
		String input2 = input.toLowerCase();
		if(input2.contentEquals(lastInput)) {
			return lastScore;
		}
		lastInput = input2;
		String regex = "\\s+"+input2+"\\s+";
		values.values().stream().forEach(value->{
			String lowerCaseValue = value.toLowerCase();
			score[0] = score[0] +
					(lowerCaseValue.trim().equalsIgnoreCase(input2.trim())?3:
						lowerCaseValue.matches(regex)?2:
							lowerCaseValue.contains(input2)?1:0);});
		metadata.values().stream().forEach(value->{
			String lowerCaseValue = value.toLowerCase();
			score[0] = score[0] +
					(lowerCaseValue.trim().equalsIgnoreCase(input2.trim())?3:
						lowerCaseValue.matches(regex)?2:
							lowerCaseValue.contains(input2)?1:0);});
		lastScore = score[0];
		return score[0];
	}

	public void addVarReportMeta(Element e) {
		e.getAllElements().stream().forEach((Element element)->{
			Element descriptionElement = element.getElementsByTag("description").first();
			if(descriptionElement!=null) {
				this.metadata.put("var_report_description", cleanText(descriptionElement.ownText()));
			}Element commentElement = element.getElementsByTag("comment").first();
			if(commentElement!=null) {
				this.metadata.put("var_report_comment", cleanText(commentElement.ownText()));
			}
		});
		buildTags();
	}

	private String cleanText(String text) {
		return text.replaceAll("<a href.*>", "").replaceAll("</a>", "").replaceAll("&#39;", "'");
	}
	
	public HashMap<String, String> getMetadata() {
		return metadata;
	}

	public void setMetadata(HashMap<String, String> metadata) {
		this.metadata = metadata;
	}

	public HashMap<String, String> getValues() {
		return values;
	}

	public void setValues(HashMap<String, String> values) {
		this.values = values;
	}

	public HashSet<String> getMetadata_tags() {
		return metadata_tags;
	}

	public void setMetadata_tags(HashSet<String> metadata_tags) {
		this.metadata_tags = metadata_tags;
	}

	public HashSet<String> getValue_tags() {
		return value_tags;
	}

	public void setValue_tags(HashSet<String> value_tags) {
		this.value_tags = value_tags;
	}
}
