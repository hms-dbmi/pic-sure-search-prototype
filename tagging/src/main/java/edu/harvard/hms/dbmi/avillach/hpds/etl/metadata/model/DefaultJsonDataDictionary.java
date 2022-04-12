package edu.harvard.hms.dbmi.avillach.hpds.etl.metadata.model;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

public class DefaultJsonDataDictionary {
	
	private String studyAccession;
	private String studyFullName;
	private String studyUrl;
	private String studyShortName;
	private Set<FormGroup> formGroups = new HashSet<FormGroup>();
	
	public class FormGroup {
		
		public String formGroupDesc;
		public String formGroupName;
		public String formName;
		public Set<Form> form = new HashSet<Form>();
		
		public FormGroup(JsonNode formGroupNode) {
			this.formGroupName = formGroupNode.has("form_group_name") ? formGroupNode.get("form_group_name").asText(): "";
			this.formGroupDesc = formGroupNode.has("form_group_description") ? formGroupNode.get("form_group_description").asText():"";
			
			if(formGroupNode.has("form") ) {
				JsonNode formNodes = formGroupNode.get("form");
				for(JsonNode formNode: formNodes) {					
					this.form.add(new Form(formNode));
				}
			}		
		}

		public FormGroup() {
			// TODO Auto-generated constructor stub
		}
		
	}
	public class Form {
		public String dataFileName;
		public String formDescription;
		public String formName;
		public VariableGroup variableGroup;
		
		public Form(JsonNode formNode) {

			this.dataFileName = formNode.has("data_file_name") ? formNode.get("data_file_name").asText() : "";
			
			this.formDescription = formNode.has("form_description") ? formNode.get("form_description").asText() : "";
			this.formName = formNode.has("form_name") ? formNode.get("form_name").asText() : "";
			
			if(formNode.has("variable_group")) {
				for(JsonNode variableGroup: formNode.get("variable_group")) {
					this.variableGroup = new VariableGroup(variableGroup);
				}
			} else {
				this.variableGroup = new VariableGroup("Default", formNode);
			}
		}
		
	}
	
	public class VariableGroup {

		public Set<Variable> variables = new HashSet<>();
		public String variableGroupDesc;
		public String variableGroupName;
		
		public VariableGroup(JsonNode variableGroupNode) {
			this.variableGroupName = variableGroupNode.has("variable_group_name") ? variableGroupNode.get("variable_group_name").asText() : "";
			this.variableGroupDesc = variableGroupNode.has("variable_group_description") ? variableGroupNode.get("variable_group_description").asText() : "";
			
			if(variableGroupNode.has("variable")) {
	
				for(JsonNode variableNode: variableGroupNode.get("variable")) {
					variables.add(new Variable(variableNode));
				}
			}
		}


		public VariableGroup(String varGroupName, JsonNode node) {
			this.variableGroupName = varGroupName;
			if(node.has("variable")) {
				
				for(JsonNode variableNode: node.get("variable")) {
					variables.add(new Variable(variableNode));
				}
			}
		}
	}
	
	public class Variable {
		public String variableId;
		public String variableName;
		public String variableType;

		public Set<VariableMetadata> variableMetadata = new HashSet<>();
		
		public Variable(JsonNode variableNode) {
			this.variableId = variableNode.has("variable_id") ? variableNode.get("variable_id").asText() : "";
			this.variableName = variableNode.has("variable_name") && !variableNode.get("variable_name").textValue().trim().isBlank() ? variableNode.get("variable_name").asText() : variableNode.get("variable_id").asText();
			this.variableType = variableNode.has("variable_type") ? variableNode.get("variable_type").asText() : "";
			
			//for(JsonNode variableMetadataNode: variableNode.get("variable_metadata")) {
			//	this.variableMetadata.add(new VariableMetadata(variableMetadataNode));
			//}
		}
	}
	
	public class VariableMetadata {
		
		public String variableDescription;
		public String variableLabelFromDataDictionary;
		
		public VariableMetadata(JsonNode variableMetadataNode) {
			this.variableDescription = variableMetadataNode.get("variable_description").asText();
			this.variableLabelFromDataDictionary = variableMetadataNode.get("variable_label_from_data_dictionary").asText();
		}
	}
	
	public String getStudyAccession() {
		return studyAccession;
	}

	public void setStudyAccession(String studyAccession) {
		this.studyAccession = studyAccession;
	}

	public String getStudyFullName() {
		return studyFullName;
	}

	public void setStudyFullName(String studyFullName) {
		this.studyFullName = studyFullName;
	}

	public String getStudyUrl() {
		return studyUrl;
	}

	public void setStudyUrl(String studyUrl) {
		this.studyUrl = studyUrl;
	}

	public String getStudyShortName() {
		return studyShortName;
	}

	public void setStudyShortName(String studyShortName) {
		this.studyShortName = studyShortName;
	}

	public Set<FormGroup> getFormGroups() {
		return formGroups;
	}

	public void setFormGroups(Set<FormGroup> formGroups) {
		this.formGroups = formGroups;
	}

	public void setFormGroups(JsonNode node) {
		node.forEach(formGroupNode -> {
			this.formGroups.add(new FormGroup(formGroupNode));
		});
	}
	
	public void setFormGroupsWOaGroup(JsonNode node) {
		
		FormGroup fg = new FormGroup();
		fg.formGroupDesc = "unknown";
				
		if(node.has("form") ) {
			JsonNode formNodes = node.get("form");
			for(JsonNode formNode: formNodes) {					
				fg.form.add(new Form(formNode));
			}
		}	
		this.formGroups.add(fg);
	}

	
}
