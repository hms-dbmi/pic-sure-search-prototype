package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultJsonDictionaryModel extends DictionaryModel {

	public static List<DefaultJsonDictionaryModel> allModels = new ArrayList<>();
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
		
		public FormGroup(JsonNode formGroupNode, DefaultJsonDictionaryModel defaultJsonDictionaryModel) {
			this.formGroupName = formGroupNode.has("form_group_name") ? formGroupNode.get("form_group_name").asText(): "";
			defaultJsonDictionaryModel.derived_group_name = this.formGroupName;
			defaultJsonDictionaryModel.derived_group_id = this.formGroupName;

			this.formGroupDesc = formGroupNode.has("form_group_description") ? formGroupNode.get("form_group_description").asText():"";
			defaultJsonDictionaryModel.derived_group_description = this.formGroupDesc;
			
			if(formGroupNode.has("form") ) {
				JsonNode formNodes = formGroupNode.get("form");
				for(JsonNode formNode: formNodes) {					
					this.form.add(new Form(formNode, defaultJsonDictionaryModel));
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
		
		public Form(JsonNode formNode, DefaultJsonDictionaryModel defaultJsonDictionaryModel) {

			this.dataFileName = formNode.has("data_file_name") ? formNode.get("data_file_name").asText() : "";
			
			this.formDescription = formNode.has("form_description") ? formNode.get("form_description").asText() : "";
			this.formName = formNode.has("form_name") ? formNode.get("form_name").asText() : "";
			
			if(formNode.has("variable_group")) {
				for(JsonNode variableGroup: formNode.get("variable_group")) {
					this.variableGroup = new VariableGroup(variableGroup, defaultJsonDictionaryModel);
				}
			} else {
				this.variableGroup = new VariableGroup("Default", formNode, defaultJsonDictionaryModel);
			}
		}
		
	}
	
	public class VariableGroup {

		public Set<Variable> variables = new HashSet<>();
		public String variableGroupDesc;
		public String variableGroupName;
		public String variableGroupId;
		
		public VariableGroup(JsonNode variableGroupNode, DefaultJsonDictionaryModel defaultJsonDictionaryModel) {
			this.variableGroupName = variableGroupNode.has("variable_group_name") ? variableGroupNode.get("variable_group_name").asText() : "";
			this.variableGroupDesc = variableGroupNode.has("variable_group_description") ? variableGroupNode.get("variable_group_description").asText() : "";
			if(variableGroupNode.has("variable")) {
	
				for(JsonNode variableNode: variableGroupNode.get("variable")) {
					variables.add(new Variable(variableNode, defaultJsonDictionaryModel));
					
				}
			}
		}


		public VariableGroup(String varGroupName, JsonNode node, DefaultJsonDictionaryModel defaultJsonDictionaryModel) {
			this.variableGroupName = varGroupName;
			if(node.has("variable")) {
				
				for(JsonNode variableNode: node.get("variable")) {
					variables.add(new Variable(variableNode, defaultJsonDictionaryModel));
				}
			}
		}
	}
	
	public class Variable {
		public String variableId;
		public String variableName;
		public String variableType;

		public Set<VariableMetadata> variableMetadata = new HashSet<>();
		
		public Variable(JsonNode variableNode, DefaultJsonDictionaryModel defaultJsonDictionaryModel) {
			this.variableId = variableNode.has("variable_id") ? variableNode.get("variable_id").asText() : "";
			defaultJsonDictionaryModel.derived_var_name = this.variableId;
			
			this.variableName = variableNode.has("variable_name") && !variableNode.get("variable_name").textValue().trim().isBlank() ? variableNode.get("variable_name").asText() : variableNode.get("variable_id").asText();
			defaultJsonDictionaryModel.derived_var_description = this.variableName;
			
			this.variableType = variableNode.has("variable_type") ? variableNode.get("variable_type").asText() : "";
			
			
			allModels.add(new DefaultJsonDictionaryModel(defaultJsonDictionaryModel));
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

	public void setFormGroups(JsonNode node, DefaultJsonDictionaryModel defaultJsonDictionaryModel) {
		node.forEach(formGroupNode -> {
			this.formGroups.add(new FormGroup(formGroupNode,defaultJsonDictionaryModel));
		});
	}
	
	public void setFormGroupsWOaGroup(JsonNode node, DefaultJsonDictionaryModel defaultJsonDictionaryModel) {
		
		FormGroup fg = new FormGroup();
		fg.formGroupDesc = "unknown";
				
		if(node.has("form") ) {
			JsonNode formNodes = node.get("form");
			for(JsonNode formNode: formNodes) {					
				fg.form.add(new Form(formNode,defaultJsonDictionaryModel));
			}
		}	
		this.formGroups.add(fg);
	}

	
	
	@Override
	public Map<String, DictionaryModel> build() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DictionaryModel> build(String[] controlFileRow, Map<String,DictionaryModel> baseDictionary) {
		String inputDirectory = controlFileRow[2];
		File studyFolder = new File(inputDirectory);
		if(!studyFolder.isFile()) {
			for(File study : new File(inputDirectory).listFiles()) {
	        	if(studyFolder.getName().contains("hrmn")) continue;
	        	if(!study.getName().endsWith("metadata.json")) continue;
		        DefaultJsonDictionaryModel dict = new DefaultJsonDictionaryModel(study.getAbsolutePath());
					//File f = new File("./data/babyhug/rawData/babyhug_metadata.json");
			}	
		}
		System.out.println("Updating base dictionary.");

		for(DefaultJsonDictionaryModel model: allModels) {
			updateBaseDictionary(baseDictionary, model);
		}
		
		return baseDictionary;
	}
	
	private void updateBaseDictionary(Map<String, DictionaryModel> baseDictionary, DefaultJsonDictionaryModel dict) {
		
		//baseDictionary.entrySet().forEach(entry -> {
			
		String dictphs = dict.derived_study_id;
		
		String dictVarId = dict.derived_var_name;
		
		//if(entry.getKey().equals("\\" + dictphs + "\\" + dictVarId + "\\")) {
		String key = "\\" + dictphs + "\\" + dictVarId + "\\";
		DictionaryModel baseModel = baseDictionary.get(key);
		if(baseModel != null) { 
		for(Field f: dict.getClass().getSuperclass().getDeclaredFields()) {
			try {
				
				f.setAccessible(true);
				
				String fieldName = f.getName();
				
				Object fieldVal = f.get(dict);
				
				if(fieldVal != null && !fieldVal.toString().isBlank()) {
					if(Arrays.asList(baseModel.getClass().getFields()).contains(f)) {
						Field fieldToSet = baseModel.getClass().getSuperclass().getDeclaredField(fieldName);
						
						fieldToSet.setAccessible(true);
						
						fieldToSet.set(baseModel, fieldVal);
					}
				}
				
			} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		baseDictionary.get("\\" + dictphs + "\\" + dictVarId + "\\").derived_study_description = dict.derived_study_description.isBlank() ? baseDictionary.get("\\" + dictphs + "\\" + dictVarId + "\\").derived_study_description: dict.derived_study_description;
		baseDictionary.get("\\" + dictphs + "\\" + dictVarId + "\\").derived_group_id = dict.studyFullName;
		
		//for(FormGroup fg: dict.formGroups) {
			//for()
		//}
		}
		//}
		//}); 
	}

	/**
	 * Builds data from the json data. 
	 */
	public DefaultJsonDictionaryModel(String filename) {
		
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readTree(new File(filename));
			node.forEach((node2) -> {
				
				this.setStudyAccession(node2.get("study_phs_number").asText());
				this.derived_study_id = node2.get("study_phs_number").asText();
				
				this.setStudyFullName(node2.get("study_name").asText());
				this.derived_study_description = node2.get("study_name").asText();
				
				this.setStudyShortName(node2.get("study").asText());
				
				this.setStudyUrl(node2.get("study_url").asText());
				this.derived_group_id = "";
				JsonNode formGroups = node2.has("form_group") ? node2.get("form_group"): null;
				// if no formgroups not given start with form element
				if(formGroups != null) {
					this.setFormGroups(formGroups,this);
				} else {
					this.setFormGroupsWOaGroup(node2,this);
				}
				//dict.setFormGroups(buildForms(formGroups));
				
			});
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public DefaultJsonDictionaryModel() {
		// TODO Auto-generated constructor stub
	}

	public DefaultJsonDictionaryModel(DefaultJsonDictionaryModel defaultJsonDictionaryModel) {
		super(defaultJsonDictionaryModel);
	}
}
