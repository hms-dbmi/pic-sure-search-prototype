package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.DictionaryModel;

public class DCCHarmonizedDictionaryModel extends DictionaryModel {

	public static List<DCCHarmonizedDictionaryModel> allModels = new ArrayList<>();
	
	public DCCHarmonizedDictionaryModel(String fileName) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			File f = new File(fileName);
			JsonNode node = mapper.readTree(f);
			
			this.derived_var_description = node.get("description").asText();
			this.derived_var_id = node.get("name").asText();
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public DCCHarmonizedDictionaryModel() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Map<String, DictionaryModel> build() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DictionaryModel> build(String[] controlFileRow, Map<String, DictionaryModel> baseDictionary) {
		String inputDirectory = controlFileRow[2];
		File studyFolder = new File(inputDirectory);
		if(!studyFolder.isFile()) {
			for(File study : new File(inputDirectory).listFiles()) {
	        	if(!study.getName().endsWith(".json")) continue;
	        	DCCHarmonizedDictionaryModel dict = new DCCHarmonizedDictionaryModel(study.getAbsolutePath());
					//File f = new File("./data/babyhug/rawData/babyhug_metadata.json");
			}	
		}
		List<DCCHarmonizedDictionaryModel> models = allModels;
		
		for(DCCHarmonizedDictionaryModel model: allModels) {
			updateBaseDictionary(baseDictionary, model);
		}
		return null;
	}

	private void updateBaseDictionary(Map<String, DictionaryModel> baseDictionary, DCCHarmonizedDictionaryModel model) {
		// TODO Auto-generated method stub
		
	}

}
