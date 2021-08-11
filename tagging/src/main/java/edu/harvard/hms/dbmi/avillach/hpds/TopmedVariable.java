package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
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
	private HashSet<String> allTagsLowercase = new HashSet<>();
	private String studyId;
	private String dtId;
	private String varId;

	public TopmedVariable(){

	}

	public TopmedVariable(TopmedDataTable topmedDataTable, Element e){
		this.metadata = new HashMap<>();
		this.values = new HashMap<>();
		e.getAllElements().stream().forEach((element)->{
			if(element.tag().getName().equalsIgnoreCase("value")) {
				this.values.put(element.attr("code"), element.ownText());
			} else {
				if(!element.tag().getName().equalsIgnoreCase("comment")) {
					this.metadata.put(element.tagName(), element.ownText());
				}
			}
		});
		this.dtId = topmedDataTable.metadata.get("id");
		this.studyId = topmedDataTable.metadata.get("study_id");
		this.varId = e.id();
		this.metadata.put("varId",e.id());
		metadata.put("study_id", studyId);
		metadata.put("dataTableId", topmedDataTable.metadata.get("id"));
		metadata.put("dataTableDescription", topmedDataTable.metadata.get("description"));
		metadata.put("dataTableName", topmedDataTable.metadata.get("name"));
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
		metadata_tags.add(studyId);
		metadata_tags.add(studyId.split("\\.")[0].toUpperCase());
		allTagsLowercase.addAll(value_tags.stream().map((tag)->{
			return tag.toLowerCase();
		}).collect(Collectors.toSet()));
		allTagsLowercase.addAll(metadata_tags.stream().map((tag)->{
			return tag.toLowerCase();
		}).collect(Collectors.toSet()));
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

	public void internStrings() {
		HashMap<String, String> metadata = new HashMap<>();
		HashMap<String, String> values = new HashMap<>();
		HashSet<String> metadata_tags = new HashSet<>();
		HashSet<String> value_tags = new HashSet<>();
		HashSet<String> allTagsLowercase = new HashSet<>();
		for(Entry<String, String> entry : this.metadata.entrySet()) {
			metadata.put(entry.getKey().intern(), entry.getValue().intern());
		}
		this.metadata = metadata;
		for(Entry<String, String> entry : this.metadata.entrySet()) {
			values.put(entry.getKey().intern(), entry.getValue().intern());
		}
		this.values = values;
		for(String entry : this.metadata_tags) {
			metadata_tags.add(entry.intern());
		}
		this.metadata_tags = metadata_tags;
		for(String entry : this.value_tags) {
			value_tags.add(entry.intern());
		}
		this.value_tags = value_tags;
		for(String entry : this.allTagsLowercase) {
			allTagsLowercase.add(entry.intern());
		}
		this.allTagsLowercase = allTagsLowercase;
	}

	private String lastInput = "";
	private double lastScore;

	double relevance(String input) {
		String inputTrimmed = input.toLowerCase().trim();
		if(inputTrimmed.contentEquals(lastInput)) {
			return lastScore;
		}
		lastInput = inputTrimmed;
		double[] score = {0};
		String[] inputs = inputTrimmed.split("\\s+");
		for(String input2 : inputs) {
			
			String regex = ".*\\s+"+input2+"\\s+.*";
			allTagsLowercase.stream().filter((tag)->{
				return tag.contains(input2);
			}).forEach((tag)->{
				score[0] = score[0] +
						(tag.equalsIgnoreCase(input2)?3:
							tag.matches(regex)?2:1);
			});
			
		}
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

	public String getDtId() {
		return dtId;
	}

	public void setDtId(String dtId) {
		this.dtId = dtId;
	}

	public String getStudyId() {
		return studyId;
	}

	public void setStudyId(String studyId) {
		this.studyId = studyId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dtId == null) ? 0 : dtId.hashCode());
		result = prime * result + ((studyId == null) ? 0 : studyId.hashCode());
		result = prime * result + ((varId == null) ? 0 : varId.hashCode());
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
		TopmedVariable other = (TopmedVariable) obj;
		if (dtId == null) {
			if (other.dtId != null)
				return false;
		} else if (!dtId.equals(other.dtId))
			return false;
		if (studyId == null) {
			if (other.studyId != null)
				return false;
		} else if (!studyId.equals(other.studyId))
			return false;
		if (varId == null) {
			if (other.varId != null)
				return false;
		} else if (!varId.equals(other.varId))
			return false;
		return true;
	}

}
