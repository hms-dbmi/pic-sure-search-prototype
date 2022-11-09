package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.jsoup.nodes.Element;

public class TopmedVariable implements Serializable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = -317988926878698761L;

	private Map<String, String> metadata;
	private List<String> values;
	@JsonIgnore
	private Set<String> metadata_tags = new HashSet<>();
	@JsonIgnore
	private Set<String> value_tags = new HashSet<>();
	@JsonIgnore
	public Set<String> allTagsLowercase = new HashSet<>();

	private String studyId;
	private String dtId;
	private String varId;
	private boolean is_categorical;
	private boolean is_continuous;

	public TopmedVariable(){
		this.metadata = new HashMap<>();
		this.values = new ArrayList<>();
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

	private String cleanText(String text) {
		return text.replaceAll("<a href.*>", "").replaceAll("</a>", "").replaceAll("&#39;", "'");
	}

	public Map<String, String> getMetadata() {
		return metadata;
	}

	public TopmedVariable setMetadata(Map<String, String> metadata) {
		this.metadata = metadata;
		return this;
	}

	public List<String> getValues() {
		return values;
	}

	public TopmedVariable setValues(List<String> values) {
		this.values = values;
		return this;
	}

	public Set<String> getMetadata_tags() {
		return metadata_tags;
	}

	public TopmedVariable setMetadata_tags(Set<String> metadata_tags) {
		this.metadata_tags = metadata_tags;
		return this;
	}

	public Set<String> getValue_tags() {
		return value_tags;
	}

	public TopmedVariable setValue_tags(Set<String> value_tags) {
		this.value_tags = value_tags;
		return this;
	}

	public String getVarId() {
		return varId;
	}

	public TopmedVariable setVarId(String varId) {
		this.varId = varId;
		return this;
	}

	public String getDtId() {
		return dtId;
	}

	public TopmedVariable setDtId(String dtId) {
		this.dtId = dtId;
		return this;
	}

	public String getStudyId() {
		return studyId;
	}

	public TopmedVariable setStudyId(String studyId) {
		this.studyId = studyId;
		return this;
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

	public TopmedVariable setIs_categorical(boolean is_categorical) {
		this.is_categorical = is_categorical;
		return this;
	}

	public boolean isIs_continuous() {
		return is_continuous;
	}

	public TopmedVariable setIs_continuous(boolean is_continuous) {
		this.is_continuous = is_continuous;
		return this;
	}

	/**
	 * Make a copy of this object.
	 *
	 * @param valueLimit maximum number of values to include in copy
	 */
	public TopmedVariable copy(int valueLimit) {
		//  TODO: Since this object is not immutable, any changes to any of its fields will be reflected in any shared copies,
		//  i.e. in TagSearchResource.fhsDictionary. We should create and use an immutable version of this object
		//  for caching and including in service responses
		TopmedVariable topmedVariable = new TopmedVariable();
		topmedVariable.metadata = ImmutableMap.copyOf(this.metadata);

		if (this.values.size() > valueLimit) {
			List<String> valuesCopy = this.values.stream()
					.limit(valueLimit)
					.collect(Collectors.toList());
			topmedVariable.values = ImmutableList.copyOf(valuesCopy);
		} else {
			topmedVariable.values = ImmutableList.copyOf(this.values);
		}
		topmedVariable.metadata_tags = ImmutableSet.copyOf(this.metadata_tags);
		topmedVariable.value_tags = ImmutableSet.copyOf(this.value_tags);
		topmedVariable.allTagsLowercase = ImmutableSet.copyOf(this.allTagsLowercase);
		topmedVariable.studyId = this.studyId;
		topmedVariable.dtId = this.dtId;
		topmedVariable.varId = this.varId;
		topmedVariable.is_categorical = this.is_categorical;
		topmedVariable.is_continuous = this.is_continuous;
		topmedVariable.lastInput = this.lastInput;
		topmedVariable.lastScore = this.lastScore;
		return topmedVariable;
	}

}
