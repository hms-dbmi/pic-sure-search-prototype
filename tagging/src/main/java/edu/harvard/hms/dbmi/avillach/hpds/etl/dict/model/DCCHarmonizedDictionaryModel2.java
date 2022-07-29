package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

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
public class DCCHarmonizedDictionaryModel2 extends DictionaryModel {

	public static List<DCCHarmonizedDictionaryModel2> allModels = new ArrayList<>();
	
	public DCCHarmonizedDictionaryModel2(String fileName) {

	}

	public DCCHarmonizedDictionaryModel2() {
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
	        	if(!study.getName().endsWith("dcc_harmonized.json")) continue;
	    		ObjectMapper mapper = new ObjectMapper();
	    		try {
	    			
	    			JsonNode node = mapper.readTree(study);
	    			
	    			ArrayNode varGroups = (ArrayNode) node.get(0).get("var_groups");
	    			
	    			Iterator<JsonNode> varGroupsIter = varGroups.elements();
	    			
	    			JsonNode vg = null;
	    			while(varGroupsIter.hasNext()) {
	    				vg = varGroupsIter.next();
	    				String groupId = vg.get("group_id").asText();
	    				Iterator<JsonNode> vars = vg.get("variable").elements();
	    				
	    				JsonNode var = null;
	    				while(vars.hasNext()) {
	    					var = vars.next();
	    					String varId = var.get("var_id").asText();
	    					
	    					String varName = var.get("var_name").asText();
	    					
		    				JsonNode meta = var.get("variable_metadata").get(0);

	    					String hpdsPath = "\\DCC Harmonized data set\\" + groupId + "\\" + varId + "\\";
	    					
	    					DictionaryModel base = baseDictionary.get(hpdsPath);
	    					
	    					base.derived_group_id = groupId;
	    					base.derived_var_id = varId;
	    					base.derived_var_name = varName;

	    					base.derived_var_description = meta.get("var_description").asText();

	    				}
	    				
	    			}
	    			
	    			
	    		} catch (JsonProcessingException e) {
	    			// TODO Auto-generated catch block
	    			e.printStackTrace();
	    		} catch (IOException e) {
	    			// TODO Auto-generated catch block
	    			e.printStackTrace();
	    		}
	        	//allModels.add(dict);
					//File f = new File("./data/babyhug/rawData/babyhug_metadata.json");
			}	
		}
		
		return null;
	}

	private void updateBaseDictionary(Map<String, DictionaryModel> baseDictionary, DCCHarmonizedDictionaryModel2 model) {

	}


}
