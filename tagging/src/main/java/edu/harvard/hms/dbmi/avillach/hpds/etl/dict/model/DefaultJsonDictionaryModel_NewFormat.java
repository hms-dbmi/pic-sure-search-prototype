package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;

/**
 * This model is used for the internal dictionaries for non-compliant studies
 * 
 * It is presumed that any studies processing internal json will use this model.
 * 
 * This class should replace the DefaultDictionaryModel once test case and other studies are migrated to new json format.
 */
public class DefaultJsonDictionaryModel_NewFormat extends DictionaryModel {

	public static List<DefaultJsonDictionaryModel_NewFormat> allModels = new ArrayList<>();
	private String studyAccession;
	private String studyFullName;
	private String studyUrl;
	private String studyShortName;
	private Set<FormGroup> formGroups = new HashSet<FormGroup>();

	public static List<String[]> DICTIONARIES_MISSING_IN_HPDS_COLUMNMETA_DATA = new ArrayList<String[]>();

	
	public class FormGroup {
		
		public String formGroupDesc;
		public String formGroupName;
		public String formName;
		public Set<Form> form = new HashSet<Form>();
		
		/**
		 * empty form group to handle if json does not contain a form group.
		 */
		public FormGroup() {
			
		};
		
		public FormGroup(JsonNode formGroupNode, DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
			//this.formGroupName = formGroupNode.has("form_group_name") ? formGroupNode.get("form_group_name").asText(): "";
			defaultJsonDictionaryModel.derived_group_name = this.formGroupName;
			defaultJsonDictionaryModel.derived_group_id = this.formGroupName;

			//this.formGroupDesc = formGroupNode.has("form_group_description") ? formGroupNode.get("form_group_description").asText():"";
			defaultJsonDictionaryModel.derived_group_description = this.formGroupDesc;
			
			if(formGroupNode.has("form") ) {
				JsonNode formNodes = formGroupNode.get("form");
				for(JsonNode formNode: formNodes) {					
					this.form.add(new Form(formNode, defaultJsonDictionaryModel));
				}
			}		
		}
		
		
	}
	public class Form {
		public String dataFileName;
		public String formDescription;
		public String formName;
		public VariableGroup variableGroup;
		
		public Form(JsonNode formNode, DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {

			//this.dataFileName = formNode.has("data_file_name") ? formNode.get("data_file_name").asText() : "";
			
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
		
		public VariableGroup(JsonNode variableGroupNode, DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
			this.variableGroupName = variableGroupNode.has("variable_group_name") ? variableGroupNode.get("variable_group_name").asText() : "";
			this.variableGroupDesc = variableGroupNode.has("variable_group_description") ? variableGroupNode.get("variable_group_description").asText() : "";
			if(variableGroupNode.has("variable")) {
	
				for(JsonNode variableNode: variableGroupNode.get("variable")) {
					variables.add(new Variable(variableNode, defaultJsonDictionaryModel));
					
				}
			}
		}


		public VariableGroup(String varGroupName, JsonNode node, DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
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

		//public Set<VariableMetadata> variableMetadata = new HashSet<>();
		
		public Variable(JsonNode variableNode, DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
			
			this.variableId = variableNode.has("variable_id") ? variableNode.get("variable_id").asText() : "";
			defaultJsonDictionaryModel.derived_var_id = this.variableId;
			defaultJsonDictionaryModel.derived_var_name = this.variableId;

			// currently variable_name is the encoded variable_id
			// and variable name is the decoded variable_id stored in variable_name
			
			this.variableName = variableNode.has("variable_name") ? variableNode.get("variable_name").asText() : "";
			defaultJsonDictionaryModel.derived_var_description = variableNode.has("variable_description")? variableNode.get("variable_description").asText() : "";
			
			// var type is derived from columnmeta data		
			this.variableType = variableNode.has("variable_type") ? variableNode.get("variable_type").asText() : "";
			
			if(variableNode.has("derived_variable_level_data")) {
				
				for(JsonNode derivedVariableLevelData: variableNode.get("derived_variable_level_data")) {
					//this.variableMetadata.add(new VariableMetadata(variableMetadataNode, defaultJsonDictionaryModel));
					//break;
					Iterator<Entry<String, JsonNode>> derivedVariableLevelDataElements = derivedVariableLevelData.fields();
					
					Entry derivedVariableLevelDataElement;
					
					while(derivedVariableLevelDataElements.hasNext()) {
						
						derivedVariableLevelDataElement = derivedVariableLevelDataElements.next();
						defaultJsonDictionaryModel.derived_variable_level_data.put(derivedVariableLevelDataElement.getKey().toString(),derivedVariableLevelDataElement.getValue().toString());
					
					}
					
					
				}
				/*
				for(JsonNode variableMetadataNode: variableNode.get("variable_metadata")) {
					this.variableMetadata.add(new VariableMetadata(variableMetadataNode, defaultJsonDictionaryModel));
					break;
				}
				*/
			} else {
				allModels.add(new DefaultJsonDictionaryModel_NewFormat(defaultJsonDictionaryModel));

			}
		}
	}
	/*
	public class VariableMetadata {
		
		public String variableDescription;
		public String variableLabelFromDataDictionary;
		
		public VariableMetadata(JsonNode variableMetadataNode, DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
			this.variableDescription = variableMetadataNode.has("variable_description") ? variableMetadataNode.get("variable_description").asText(): "";
			//this.variableLabelFromDataDictionary =  variableMetadataNode.has("variable_label_from_data_dictionary") ? variableMetadataNode.get("variable_label_from_data_dictionary").asText() : "";
		    //defaultJsonDictionaryModel.derived_var_description = this.variableDescription;
			allModels.add(new DefaultJsonDictionaryModel_NewFormat(defaultJsonDictionaryModel));

		}
	}
	*/
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

	public void setFormGroups(JsonNode node, DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
		node.forEach(formGroupNode -> {
			this.formGroups.add(new FormGroup(formGroupNode,defaultJsonDictionaryModel));
		});
	}
	
	public void setFormGroupsWOaGroup(JsonNode node, DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
		
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
	        	DefaultJsonDictionaryModel_NewFormat dict = new DefaultJsonDictionaryModel_NewFormat(study.getAbsolutePath());
			}	
		}
		System.out.println("Looking for missing dictionaries");

		reportMissingDictionaries(baseDictionary,controlFileRow);

		System.out.println("Updating base dictionary.");
		
		for(DefaultJsonDictionaryModel_NewFormat model: allModels) {
			updateBaseDictionary(baseDictionary, model);
		}
		
		reportMissingColumnmeta();
		
		return baseDictionary;
	}
	
	private void reportMissingColumnmeta() {
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get("/usr/local/docker-config/search/" + "Missing_Columnmeta.csv"), StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
			
			CSVWriter csvwriter = new CSVWriter(writer);
	
			for(String[] line: DICTIONARIES_MISSING_IN_HPDS_COLUMNMETA_DATA) {
				csvwriter.writeNext(line);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
			
	private void reportMissingDictionaries(Map<String, DictionaryModel> baseDictionary, String[] controlFileRow) {
		
		Set<String> allVariableNamesInDictionary = collectVariableNames();
		
		String phs = controlFileRow[1];
		
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get("/usr/local/docker-config/search/" + "Missing_Dictionary_Entries.csv"), StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
			
			CSVWriter csvwriter = new CSVWriter(writer);
			
			for(Entry<String,DictionaryModel> baseEntry: baseDictionary.entrySet()) {
				if(!phs.equals(baseEntry.getValue().derived_study_id)) continue;
				if(!allVariableNamesInDictionary.contains(baseEntry.getValue().derived_var_name)) {
					csvwriter.writeNext(new String[]{ baseEntry.getValue().derived_study_abv_name, baseEntry.getValue().derived_study_id, baseEntry.getValue().derived_var_name });
				}
			}
			csvwriter.flush();

			csvwriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get("/usr/local/docker-config/search/" + phs + "_Dictionary_Variable_Names.csv"), StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
			CSVWriter csvwriter = new CSVWriter(writer);
			for(String varName: allVariableNamesInDictionary) {
				csvwriter.writeNext(new String[] { varName });
			}
			csvwriter.flush();

			csvwriter.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private Set<String> collectVariableNames() {
		Set<String> set = new HashSet<String>();
		
		for(DefaultJsonDictionaryModel_NewFormat model: allModels) {
			set.add(model.derived_var_name);
		}
		
		return set;
	}

	private void updateBaseDictionary(Map<String, DictionaryModel> baseDictionary, DefaultJsonDictionaryModel_NewFormat dict) {

		//baseDictionary.entrySet().forEach(entry -> {
			
		String dictphs = dict.derived_study_id;
		
		String dictVarId = dict.derived_var_id;
		
		//if(entry.getKey().equals("\\" + dictphs + "\\" + dictVarId + "\\")) {
		String key = "\\" + dictphs + "\\" + dictVarId + "\\";
		
		DictionaryModel baseModel = null;
		
		if(baseDictionary.containsKey(key)) {
			baseModel = baseDictionary.get(key);
		} else {
			key = "\\" + dictphs + "\\" + dictVarId.toUpperCase() + "\\";
			
			if(baseDictionary.containsKey(key)) {
				baseModel = baseDictionary.get(key);
			} else {
				key = "\\" + dictphs + "\\" + dictVarId.toLowerCase() + "\\";
				
				if(baseDictionary.containsKey(key)) {
					baseModel = baseDictionary.get(key);
				} else {
					DICTIONARIES_MISSING_IN_HPDS_COLUMNMETA_DATA.add(new String[] {dict.derived_study_abv_name, dict.derived_study_id, dict.derived_var_id});
					System.out.println(dict.derived_var_id + " not found in columnMeta.csv - ensure the variable ids in metadata and columnmeta match");
				}
			}
		}
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
		
		baseDictionary.get(key).derived_study_description = dict.derived_study_description.isBlank() ? baseDictionary.get("\\" + dictphs + "\\" + dictVarId + "\\").derived_study_description: dict.derived_study_description;
		baseDictionary.get(key).derived_group_id = dict.studyFullName;
		
		
		}
	
	}

	/**
	 * Builds data from the json data. 
	 */
	public DefaultJsonDictionaryModel_NewFormat(String filename) {
		
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

	public DefaultJsonDictionaryModel_NewFormat() {
		// TODO Auto-generated constructor stub
	}

	public DefaultJsonDictionaryModel_NewFormat(DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
		super(defaultJsonDictionaryModel);
	}

	@Override
	public Map<String, DictionaryModel> build(Map<String, DictionaryModel> baseDictionary) {
		// TODO Auto-generated method stub
		return null;
	}
}
