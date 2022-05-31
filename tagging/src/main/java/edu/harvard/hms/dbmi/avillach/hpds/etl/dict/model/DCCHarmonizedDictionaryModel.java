package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Pretty simple model the dbgap dictionaries do not contain much information
 * 
 * Would be great to generate our own dictionaries using the DefaultJsonDictionaryModel which is our internal
 * dictionaries that we generate.
 * 
 * This can then be deprecated.
 * @author Tom
 *
 */
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
	        	
	        	allModels.add(dict);
					//File f = new File("./data/babyhug/rawData/babyhug_metadata.json");
			}	
		}
		
		for(DCCHarmonizedDictionaryModel model: allModels) {
			updateBaseDictionary(baseDictionary, model);
		}
		return null;
	}

	private void updateBaseDictionary(Map<String, DictionaryModel> baseDictionary, DCCHarmonizedDictionaryModel model) {
		String varId = model.derived_var_id;
		
		Map<String,String> pathLookup = harmonizedPathLookup(baseDictionary);
		
		if(pathLookup.containsKey(varId)) {
			String hpdsPath = pathLookup.get(varId);
			DictionaryModel dm = baseDictionary.get(hpdsPath);
			String groupId = hpdsPath.split("\\\\")[2];
			
			dm.derived_group_id = groupId;
			dm.derived_var_description = model.derived_var_description;

		}
	}

	private Map<String, String> harmonizedPathLookup(Map<String, DictionaryModel> baseDictionary) {
		Map<String,String> lookup = new HashMap<>();
		
		baseDictionary.entrySet().stream().forEach(baseD -> {
			String root = baseD.getKey().split("\\\\")[1];
			
			if(root.contains("DCC Harmonized data set")) {
				
				DictionaryModel dm = baseD.getValue();
				
				lookup.put(dm.derived_var_id,dm.columnmeta_hpds_path);
			}
			
		});
		return lookup;
	}

}
