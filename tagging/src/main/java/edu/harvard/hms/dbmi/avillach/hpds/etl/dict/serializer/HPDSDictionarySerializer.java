package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.serializer;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.DictionaryModel;

/**
 * 
 * 
 *
 */
public class HPDSDictionarySerializer {
	
	// Dictionary object that will be serialized and written
	private TreeMap<String, TopmedDataTable> hpdsDictionary = new  TreeMap<String, TopmedDataTable>();
	
	// use this object to explicitly ignore any metadata tags that you do not want generated.
	private static List<String> IGNORE_META_KEYS = List.of(
			"hashed_var_id"
			);
	
	/**
	 * This is the public method that can be called to serialize DictionaryModel Objects into 
	 * a useable javabin for search 
	 * 
	 * @param dictionaries
	 * @return
	 */
	public TreeMap<String, TopmedDataTable> serialize(Map<String, DictionaryModel> dictionaries) {
		//TODO can stream dictionaries out here instead building the entire dictionary then writing it
		// dictionary object will be very large as the tag map grows.

		for(Entry<String,DictionaryModel> entry: dictionaries.entrySet()) {

			buildDataTable(entry);
		}
		// build tags
		buildTags();
		return hpdsDictionary;
	}
	
	/**
	 * 
	 * This method will build the metadata tags for new search.
	 * 
	 * 
	 * 
	 */
	private void buildTags() {
		for(Entry<String, TopmedDataTable> hpdsDictEntry: hpdsDictionary.entrySet()) {
			
			hpdsDictEntry.getValue().variables.forEach((varid, var) -> {
				var.getMetadata().forEach((k,v) -> {
					try {
						if(IGNORE_META_KEYS.contains(k)) return;
						TopmedVariable tvMethods = TopmedVariable.class.getDeclaredConstructor().newInstance();
						
						// phs to upper and lower
						if(!var.getStudyId().isBlank()) {
							// cannot filter lowercase objects as filter tags always returns an uppercases value
							// which is fine as we do not want to filter any study ids.
							var.getMetadata_tags().add(var.getStudyId().toLowerCase());
							var.getMetadata_tags().addAll(tvMethods.filterTags(var.getStudyId().toUpperCase()));
						}
						
						// pht to upper and lower
						if(!var.getDtId().isBlank()) {
							var.getMetadata_tags().add(var.getDtId().toLowerCase());
							var.getMetadata_tags().addAll(tvMethods.filterTags(var.getDtId().toUpperCase()));
						}
						
						
						// phv to upper and lower
						var.getMetadata_tags().add(var.getVarId().toLowerCase());
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getVarId().toUpperCase()));
												
						// data type
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getMetadata().get("columnmeta_data_type").toUpperCase()));
						
						// variable encoded name
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getMetadata().get("columnmeta_name").toUpperCase()));
						
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getMetadata().get("derived_var_description").toUpperCase()));
						
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getMetadata().get("derived_study_abv_name").toUpperCase()));
						
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getMetadata().get("derived_study_description").toUpperCase()));
						
						
					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException | NoSuchMethodException | SecurityException e) {
						// TODO exception handling
						e.printStackTrace();
					}
				});
				// add values to metadata tags and value tags
				for(String value: var.getValues().values()) {
					try {
						var.getValue_tags().addAll(TopmedVariable.class.getDeclaredConstructor().newInstance().filterTags(value.toUpperCase()));
						var.getMetadata_tags().addAll(TopmedVariable.class.getDeclaredConstructor().newInstance().filterTags(value.toUpperCase()));
					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException | NoSuchMethodException | SecurityException e) {
						e.printStackTrace();
					}
		
				}
				for(String valuesTolower: var.getValue_tags()) {
					var.allTagsLowercase.add(valuesTolower.toLowerCase());
				}
				for(String metatagsTolower: var.getMetadata_tags()) {
					var.allTagsLowercase.add(metatagsTolower.toLowerCase());
				}
			});
			hpdsDictEntry.getValue().generateTagMap();
		}		
	}

	/**
	 * This will build the dictionary data table and it's associated variables.
	 * 
	 * The code block for backward compatibility can be deprecated as the data model 
	 * matures.
	 * 
	 * @param entry
	 */
	private void buildDataTable(Entry<String, DictionaryModel> entry) {
		TopmedDataTable dt = new TopmedDataTable();
		TopmedVariable var = new TopmedVariable();
		DictionaryModel dm = entry.getValue();
		
		Field[] fields = dm.getClass().getSuperclass().getDeclaredFields();
		try {
			for(Field field: fields) {
				if(field.get(dm) != null && !String.valueOf(field.get(dm)).equalsIgnoreCase("null")){
					var.getMetadata().put(field.getName(), String.valueOf(field.get(dm)));
				} else {
					var.getMetadata().put(field.getName(), "");

				}; 
					
				
			}
		} catch (IllegalArgumentException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Backwards compatibility variables
		// could move this to a method that is more easily deprecated.
		dm.derived_group_id = dm.derived_group_id == null ? "All Variables":dm.derived_group_id;
		dm.derived_group_id = dm.derived_group_id.isBlank() ? "All Variables":dm.derived_group_id;

		var.setVarId(dm.derived_var_id.isBlank() ? "": dm.derived_var_id.split("\\.")[0]);
		
		var.setDtId(dm.derived_group_id.isBlank() ? "All Variables": dm.derived_group_id.split("\\.")[0]);
		
		var.setIs_categorical(dm.columnmeta_data_type.equals("categorical"));
		
		var.setIs_continuous(dm.columnmeta_data_type.equals("continuous"));
		
		var.setStudyId(dm.derived_study_id.isBlank() ? "": dm.derived_study_id.split("\\.")[0]);
		
		if(dm.columnmeta_data_type.equals("continuous")) {
			var.getMetadata().put("min", dm.getColumnmeta_min());
			var.getMetadata().put("max", dm.getColumnmeta_max());
		}
	
		var.getMetadata().put("columnmeta_study_id", dm.derived_study_id.isBlank() ? "": dm.derived_study_id.split("\\.")[0]);
		
		var.getMetadata().put("columnmeta_var_group_id", dm.derived_group_id.isBlank() ? "": dm.derived_group_id.split("\\.")[0]);
		
		var.getMetadata().put("columnmeta_var_id", dm.derived_var_id.split("\\.")[0]);
		
		var.getMetadata().put("columnmeta_name", dm.derived_var_name);
		
		var.getMetadata().put("columnmeta_var_group_description", dm.derived_group_description);
		
		var.getMetadata().put("columnmeta_description", dm.derived_var_description);
		
		var.getMetadata().put("description", dm.derived_var_description.isBlank() ? dm.derived_var_name: dm.derived_var_description);
		
		var.getMetadata().put("columnmeta_HPDS_PATH", dm.columnmeta_hpds_path);
		
		var.getMetadata().put("HPDS_PATH", dm.columnmeta_hpds_path);
		
		dt.metadata.put("study_description", dm.derived_study_description);

		dt.metadata.put("columnmeta_study_id", dm.derived_study_id.split("\\.")[0]);

		String[] dictKeyArr = entry.getKey().substring(1).split("\\\\");
		
		String dictKey;
		
		if(var.getStudyId().equals("_studies_consents")) {
		
			if(dictKeyArr.length == 3) {
				dictKey = dictKeyArr[0] + "_" + dictKeyArr[1] + "_" + dictKeyArr[2];
			} else if(dictKeyArr.length == 2) {
				dictKey = dictKeyArr[0] + "_" + dictKeyArr[1];
			} else {
				dictKey = dictKeyArr[0];
			}
			
		} else {
			 dictKey = var.getStudyId() + "_" + var.getDtId();
		}

		String varKey = dm.derived_var_id.split("\\.")[0];
		
		if(entry.getValue().columnmeta_data_type.equals("categorical")) {
		
			if(entry.getValue().values.isEmpty()) {
				System.err.println("CATEGORICAL VARIABLE HAS NO VALUES! = " + entry.getKey());
			} else {
				for(String value: entry.getValue().values) {
					
					var.getValues().put(value,value);
				
				}
			}
		}
		
		// removing counts 
		var.getMetadata().remove("columnmeta_observation_count");
		var.getMetadata().remove("columnmeta_patient_count");
		
		
		if(hpdsDictionary.containsKey(dictKey)) {
			
			dt = hpdsDictionary.get(dictKey);
			
			if(dt.variables.containsKey(varKey)) {
				System.err.println("VARIABLE HAS ALREADY BEEN CREATED POSSIBLE DUPLICATES = " + varKey);
			} else {
				dt.variables.put(varKey, var);
			}

		} else {
			dt.variables.put(varKey, var);
			hpdsDictionary.put(dictKey, dt);
		}
	}

}