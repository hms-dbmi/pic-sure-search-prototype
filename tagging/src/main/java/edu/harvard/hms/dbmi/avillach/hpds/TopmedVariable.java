package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jsoup.nodes.Element;

import edu.harvard.hms.dbmi.avillach.hpds.etl.RawDataImporter.ColumnMetaCSVRecord;

public class TopmedVariable implements Serializable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1802723843452332984L;

	private static final List<String> EXCLUDED_WORDS_LIST = List.of(
			"a",
			"about",
			"again",
			"AH",
			"all",
			"almost",
			"also",
			"although",
			"always",
			"among",
			"an",
			"and",
			"another",
			"any",
			"are",
			"as",
			"associated",
			"associated",
			"at",
			"be",
			"because",
			"been",
			"before",
			"being",
			"between",
			"both",
			"broad",
			"but",
			"by",
			"calculated",
			"can",
			"cardia",
			"could",
			"data",
			"decimal",
			"derived",
			"desaturation",
			"descriptions",
			"dictionaries",
			"did",
			"do",
			"documentation",
			"documents",
			"does",
			"done",
			"dopm",
			"due",
			"during",
			"each ",
			"ehough",
			"either",
			"encoded",
			"end",
			"enum",
			"especially",
			"etc",
			"extracted",
			"fhs",
			"find",
			"format",
			"format",
			"found",
			"framingham",
			"from",
			"further",
			"getting",
			"had",
			"happen",
			"has",
			"have",
			"having ",
			"here",
			"how",
			"however",
			"i",
			"if",
			"in",
			"inserted",
			"institute",
			"integer",
			"into",
			"is",
			"it",
			"its",
			"itself",
			"just",
			"kg",
			"km",
			"left",
			"made",
			"main",
			"mainly",
			"make",
			"may",
			"me",
			"mg",
			"might",
			"might",
			"ml",
			"mm",
			"most",
			"mostly",
			"must",
			"nearly",
			"neither",
			"nhlbi",
			"no",
			"nor",
			"not",
			"now",
			"numeric",
			"NWD100097",
			"obtained",
			"of",
			"often",
			"often",
			"often",
			"on",
			"one",
			"or",
			"our",
			"overall",
			"participant",
			"people",
			"perhaps",
			"please",
			"pmid",
			"position",
			"possible",
			"probably",
			"quite",
			"rather",
			"really",
			"reason",
			"regarding",
			"repository",
			"right",
			"sas",
			"sb",
			"seem",
			"seen",
			"several",
			"should",
			"show",
			"showed",
			"shown",
			"shows",
			"significantly",
			"since",
			"slice",
			"so",
			"some",
			"study",
			"submitted",
			"such",
			"sure",
			"system",
			"than",
			"that",
			"the",
			"their",
			"them",
			"then",
			"there",
			"therefore",
			"these",
			"they",
			"this",
			"those",
			"through",
			"thus",
			"time",
			"to",
			"too",
			"uab",
			"up",
			"upon",
			"values",
			"various",
			"very",
			"was",
			"we",
			"were",
			"what",
			"when",
			"whi",
			"which",
			"while",
			"with",
			"within",
			"without",
			"would",
			"XX",
			"XXXXX",
			"XXXXXXXXXXXXXXXXXXXX",
			"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
			"yes"
			);
	private HashMap<String, String> metadata = new HashMap<String, String>();
	private HashMap<String, String> values = new HashMap<String, String>();
	private HashSet<String> metadata_tags = new HashSet<>();
	private HashSet<String> value_tags = new HashSet<>();
	public HashSet<String> allTagsLowercase = new HashSet<>();

	private String studyId;
	private String dtId;
	private String varId;
	private boolean is_categorical;
	private boolean is_continuous;

	public TopmedVariable(){
		this.metadata = new HashMap<>();
		this.values = new HashMap<>();
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


	public TopmedVariable(TopmedDataTable topmedDataTable, ColumnMetaCSVRecord csvr) {
		this.metadata = new HashMap<>();
		this.values = new HashMap<>();
		
		if(csvr.categorical) {
			for(String value: csvr.categoryValues) {
				value = value.trim();
				this.values.put(value, value);
				this.value_tags.add(value);
				this.allTagsLowercase.add(value.toLowerCase());
			}
		} else {
			this.values = new HashMap<>();
			this.value_tags = new HashSet<>();
			this.allTagsLowercase = new HashSet<>();
			metadata.put("columnmeta_min", String.valueOf(csvr.min));
			metadata.put("columnmeta_max", String.valueOf(csvr.max));
		}
		this.dtId = topmedDataTable.metadata.get("columnmeta_id");
		this.studyId = topmedDataTable.metadata.get("columnmeta_study_id");
		this.is_categorical = csvr.categorical ? true: false;
		this.is_continuous = csvr.categorical ? false: true;
		metadata.put("columnmeta_study_id", studyId);
		metadata.put("columnmeta_var_group_id", topmedDataTable.metadata.get("columnmeta_id"));
		metadata.put("columnmeta_observation_count", String.valueOf(csvr.observationCount));
		metadata.put("columnmeta_var_group_description", topmedDataTable.metadata.get("columnmeta_description"));
		metadata.put("columnmeta_patient_count", String.valueOf(csvr.patientCount));
		metadata.put("columnmeta_HPDS_PATH", csvr.name);
		metadata.put("columnmeta_data_type", csvr.categorical ? "categorical": "continuous");
		String[] patharr = csvr.name.substring(1,csvr.name.length() - 1).split("\\\\");
		
		if(patharr.length == 4) {
			this.varId = patharr[2];
			metadata.put("columnmeta_var_id", patharr[3]);
			metadata.put("columnmeta_name", patharr[2]);
			metadata.put("columnmeta_description", patharr[3]);
			//metadata.put("description", patharr[3]);

		} 
		if(patharr.length == 3) {
			this.varId = patharr[2];
			metadata.put("columnmeta_var_id", patharr[2]);
			metadata.put("columnmeta_name", patharr[2]);
			metadata.put("columnmeta_description", patharr[2]);
			//metadata.put("description", patharr[2]);

		} 
		if(patharr.length == 2) {
			this.varId = patharr[1];
			metadata.put("columnmeta_var_id", patharr[1]);
			metadata.put("columnmeta_name", patharr[1]);
			metadata.put("columnmeta_description", patharr[1]);
			//metadata.put("description", patharr[1]);

		} 
		if(patharr.length == 1) {
			this.varId = patharr[0];
			metadata.put("columnmeta_var_id", patharr[0]);
			metadata.put("columnmeta_name", patharr[0]);
			metadata.put("columnmeta_description", patharr[0]);
			//metadata.put("description", patharr[0]);

		}
		
		for(String metaKey : this.metadata.keySet()) {
			this.metadata_tags.add(metaKey);
			this.allTagsLowercase.add(metaKey.toLowerCase());
		}
		
		
	}

	private void determineVariableType() {
		String type = null;
		if(this.metadata.containsKey("calculated_type") && !this.metadata.get("calculated_type").isEmpty()) {
			type = this.metadata.get("calculated_type");
		}else if(this.metadata.containsKey("type") && !this.metadata.get("type").isEmpty()) {
			type = this.metadata.get("type");
		}else if(this.metadata.containsKey("reported_type") && !this.metadata.get("reported_type").isEmpty()) {
			type = this.metadata.get("reported_type");
		}
		if(type.equalsIgnoreCase("decimal")
				||type.equalsIgnoreCase("numeric")
				||type.equalsIgnoreCase("integer")
				||type.equalsIgnoreCase("Numeral")
				||type.equalsIgnoreCase("decimal, encoded")
				||type.equalsIgnoreCase("decimal, encoded value")
				||type.equalsIgnoreCase("number")
				||type.equalsIgnoreCase("Num")
				||type.equalsIgnoreCase("real, encoded value")
				||type.equalsIgnoreCase("1")
				||type.equalsIgnoreCase("9")
				) {
			this.setIs_categorical(false);
			this.setIs_continuous(true);
			return;
		}else if(type.equalsIgnoreCase("encoded")  
				|| type.equalsIgnoreCase("string") 
				|| type.equalsIgnoreCase("string (numeral)") 
				|| type.equalsIgnoreCase("char") 
				|| type.equalsIgnoreCase("coded") 
				|| type.equalsIgnoreCase("encoded,string") 
				|| type.equalsIgnoreCase("encoded value") 
				|| type.equalsIgnoreCase("string, encoded value") 
				|| type.equalsIgnoreCase("year") 
				|| type.equalsIgnoreCase("DATETIME") 
				|| type.equalsIgnoreCase("character") 
				|| type.equalsIgnoreCase("enum_integer") 
				|| type.equalsIgnoreCase("empty field") 
				|| type.equalsIgnoreCase("encoded values") 
				|| type.equalsIgnoreCase("integer, encoded value")
				|| type.equalsIgnoreCase("integer, encoded") 
				) {
			this.setIs_categorical(true);
			this.setIs_continuous(false);
			return;
		}
		throw new RuntimeException("Could not determine type of variable : " + varId + " : " + this.metadata.get("type") + " : " + this.metadata.get("calculated_type") + " : " + this.metadata.get("reported_type"));
	}

	public void buildTags() {
		for(Entry<String, String> entry : metadata.entrySet()) {
			if(!entry.getKey().contentEquals("dataTableDescription")
					&& !entry.getKey().contentEquals("dataTableName")
					) {
				metadata_tags.addAll(filterTags(entry.getValue()));
			}
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

	public List<String> filterTags(String value) {
		return Arrays.asList(value.split("[\\s\\p{Punct}]"))
				.stream().filter((val2)->{
					return val2.length() > 1 
							&& !val2.isBlank()
							&& !val2.matches("^\\d+$")
							&& !EXCLUDED_WORDS_LIST.stream().anyMatch(val2::equalsIgnoreCase)
							//&& !EXCLUDED_WORDS_LIST.contains(val2.toUpperCase()) 
							&& !val2.toUpperCase().matches("^V\\d+$");}).map((String var)->{
								return var.toUpperCase();}).collect(Collectors.toList());
	}

	private String lastInput = "";
	private double lastScore;

	synchronized double relevance(String input) {
		String inputTrimmed = input.toLowerCase().trim();
		if(inputTrimmed.contentEquals(lastInput)) {
			return lastScore;
		}
		lastInput = inputTrimmed;
		double[] score = {0};
		String[] inputs = inputTrimmed.split("[\\s\\p{Punct}]+");
		for(String input2 : inputs) {
			Stream<String> tagsContainingInput = allTagsLowercase.stream().filter((tag)->{
				return tag.contains(input2);
			});
			tagsContainingInput.forEach((tag)->{
				score[0] = score[0] +
						(tag.contentEquals(input2)?100:1);
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
		e.attributes().forEach((attr)->{
			metadata.put(attr.getKey(), attr.getValue());
		});
		buildTags();
		determineVariableType();
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

	public String getVarId() {
		return varId;
	}

	public void setVarId(String varId) {
		this.varId = varId;
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

	public boolean isIs_categorical() {
		return is_categorical;
	}

	public void setIs_categorical(boolean is_categorical) {
		this.is_categorical = is_categorical;
	}

	public boolean isIs_continuous() {
		return is_continuous;
	}

	public void setIs_continuous(boolean is_continuous) {
		this.is_continuous = is_continuous;
	}

}
