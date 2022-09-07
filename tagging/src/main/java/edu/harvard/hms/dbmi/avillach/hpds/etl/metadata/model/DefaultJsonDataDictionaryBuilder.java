package edu.harvard.hms.dbmi.avillach.hpds.etl.metadata.model;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import edu.harvard.hms.dbmi.avillach.hpds.etl.metadata.model.DefaultJsonDataDictionary.Form;
import edu.harvard.hms.dbmi.avillach.hpds.etl.metadata.model.DefaultJsonDataDictionary.FormGroup;
import edu.harvard.hms.dbmi.avillach.hpds.etl.metadata.model.DefaultJsonDataDictionary.Variable;

public class DefaultJsonDataDictionaryBuilder {
		
	public static TreeMap<String, TopmedDataTable> build(File f) {
		TreeMap<String, TopmedDataTable> jsonHPDSDictionary = new TreeMap<>();
		DefaultJsonDataDictionary dict = new DefaultJsonDataDictionary();

		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readTree(f);
			node.forEach((node2) -> {
				
				dict.setStudyAccession(node2.get("study_phs_number").asText());
				dict.setStudyFullName(node2.get("study_name").asText());
				dict.setStudyShortName(node2.get("study").asText());
				dict.setStudyUrl(node2.get("study_url").asText());
				
				JsonNode formGroups = node2.has("form_group") ? node2.get("form_group"): null;
				// if no formgroups not given start with form element
				if(formGroups != null) {
					dict.setFormGroups(formGroups);
				} else {
					dict.setFormGroupsWOaGroup(node2);
				}
				//dict.setFormGroups(buildForms(formGroups));
				
			});
			jsonHPDSDictionary.put(dict.getStudyAccession(), buildTopmedDataTable(dict));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return jsonHPDSDictionary;
				
	}

	private static TopmedDataTable buildTopmedDataTable(DefaultJsonDataDictionary dict) {
		TopmedDataTable dataTable = new TopmedDataTable();
		dataTable.metadata = new TreeMap<>();
		dataTable.variables = new TreeMap<>();

		try {
			for(Field field : dict.getClass().getDeclaredFields()) {
				field.setAccessible(true);

				if(!field.getName().equals("formGroups")) {
			
					dataTable.metadata.put(field.getName(), field.get(dict).toString());
			
				} else {
					for(FormGroup fg: (Set<FormGroup>) field.get(dict)) {
						for(Form form: fg.form) {
							for(Variable var: form.variableGroup.variables) {
								//for(Field varField : var.getClass().getDeclaredFields()) {
								TopmedVariable topmedVar = new TopmedVariable();
								topmedVar.setDtId(dict.getStudyAccession());
								topmedVar.setVarId(var.variableId);
								topmedVar.getMetadata().put("columnmeta_description",var.variableName);
								dataTable.variables.put(var.variableId, topmedVar);
							}
						}
					}
					
				}
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//if(!dict.getStudyFullName().isBlank()) dataTable.metadata.put("study, value)
		return dataTable;
	}

	public static TreeMap<String, String> buildHarmonized(File file) {
		TreeMap<String, String> harmonized = new TreeMap<>();
		DefaultJsonDataDictionary dict = new DefaultJsonDataDictionary();

		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readTree(file);
			
			String varName = node.get("name").asText();
			String varDesc = node.get("description").asText();
			
			harmonized.put(varName, varDesc);
			
			/*
			node.forEach((node2) -> {
				
				dict.setStudyAccession("DCC Harmonized data set");
				dict.setStudyFullName("DCC Harmonized data set");
				dict.setStudyShortName("");
				dict.setStudyUrl("");
				
				JsonNode formGroups = node2.has("form_group") ? node2.get("form_group"): null;
				// if no formgroups not given start with form element
				if(formGroups != null) {
					dict.setFormGroups(formGroups);
				} else {
					dict.setFormGroupsWOaGroup(node2);
				}
				//dict.setFormGroups(buildForms(formGroups));
				
			});*/
			//jsonHPDSDictionary.put(dict.getStudyAccession(), buildTopmedDataTable(dict));
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return harmonized;
	}
	
	
}
