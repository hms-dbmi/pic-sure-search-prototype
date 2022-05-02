package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

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

public class HPDSDictionarySerializer {
	
	private static TreeMap<String, TopmedDataTable> hpdsDictionary = new  TreeMap<String, TopmedDataTable>();
	
	private static List<String> IGNORE_META_KEYS = List.of(
			"hashed_var_id"
			);
	
	public static TreeMap<String, TopmedDataTable> serialize(Map<String, DictionaryModel> dictionaries) {
		for(Entry<String,DictionaryModel> entry: dictionaries.entrySet()) {

			buildDataTable(entry);
		}
		// build tags
		buildTags();
		TreeMap<String, TopmedDataTable> test = hpdsDictionary;
		return hpdsDictionary;
	}

	private static void buildTags() {
		for(Entry<String, TopmedDataTable> hpdsDictEntry: hpdsDictionary.entrySet()) {
			
			hpdsDictEntry.getValue().variables.forEach((varid, var) -> {
				var.getMetadata().forEach((k,v) -> {
					try {
						if(IGNORE_META_KEYS.contains(k)) return;
						TopmedVariable tvMethods = TopmedVariable.class.getDeclaredConstructor().newInstance();
						// * in order to get the app working I am hard setting the metadata_tags that created.
						// micing the metadata_tags in live integration
						// phs to upper and lower
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getStudyId().toLowerCase()));
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getStudyId().toUpperCase()));
						
						// pht to upper and lower
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getDtId().toLowerCase()));
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getDtId().toUpperCase()));
						
						// phv to upper only!
						//var.getMetadata_tags().addAll(tvMethods.filterTags(var.getVarId().toLowerCase()));
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getVarId().toUpperCase()));
												
						// data type
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getMetadata().get("columnmeta_data_type").toUpperCase()));
						
						// variable encoded name
						var.getMetadata_tags().addAll(tvMethods.filterTags(var.getMetadata().get("columnmeta_name").toUpperCase()));
						
						//var.getMetadata_tags().addAll(TopmedVariable.class.getDeclaredConstructor().newInstance().filterTags(v));
						/*
						var.getMetadata_tags().add(var.getDtId());
						var.getMetadata_tags().add(var.getDtId().split("\\.")[0]);
						var.getMetadata_tags().add(var.getDtId().split("\\.")[0].toUpperCase());
						var.getMetadata_tags().add(var.getStudyId());
						var.getMetadata_tags().add(var.getStudyId().split("\\.")[0]);
						var.getMetadata_tags().add(var.getStudyId().split("\\.")[0].toUpperCase());
						var.getMetadata_tags().add(var.getVarId());
						var.getMetadata_tags().add(var.getVarId().split("\\.")[0]);
						var.getMetadata_tags().add(var.getVarId().split("\\.")[0].toUpperCase());
						*/
						for(String valuesTolower: var.getValue_tags()) {
							var.allTagsLowercase.add(valuesTolower.toLowerCase());
						}
						for(String metatagsTolower: var.getMetadata_tags()) {
							var.allTagsLowercase.add(metatagsTolower.toLowerCase());
						}
					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException | NoSuchMethodException | SecurityException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});
				for(String value: var.getValues().values()) {
					try {
						var.getValue_tags().addAll(TopmedVariable.class.getDeclaredConstructor().newInstance().filterTags(value));
					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException | NoSuchMethodException | SecurityException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		
				}
			});
			hpdsDictEntry.getValue().generateTagMap();
		}		
	}

	private static void buildDataTable(Entry<String, DictionaryModel> entry) {
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
		var.setVarId(dm.derived_var_id.split("\\.")[0]);
		var.setDtId(dm.derived_group_id.split("\\.")[0]);
		var.setIs_categorical(dm.columnmeta_data_type.equals("categorical"));
		var.setIs_continuous(dm.columnmeta_data_type.equals("continuous"));
		var.setStudyId(dm.derived_study_id.split("\\.")[0]);
		if(dm.columnmeta_data_type.equals("continuous")) {
			var.getMetadata().put("min", dm.getColumnmeta_min());
			var.getMetadata().put("max", dm.getColumnmeta_max());
		}
	
		var.getMetadata().put("columnmeta_study_id", dm.derived_study_id.split("\\.")[0]);
		var.getMetadata().put("columnmeta_var_group_id", dm.derived_group_id.split("\\.")[0]);
		var.getMetadata().put("columnmeta_var_id", dm.derived_var_id.split("\\.")[0]);
		var.getMetadata().put("columnmeta_name", dm.derived_var_name);
		var.getMetadata().put("columnmeta_var_group_description", dm.derived_group_description);
		var.getMetadata().put("columnmeta_description", dm.derived_var_description);
		var.getMetadata().put("description", dm.derived_var_description.isBlank() ? dm.derived_var_name: dm.derived_var_description);
		var.getMetadata().put("columnmeta_HPDS_PATH", dm.columnmeta_hpds_path);
		var.getMetadata().put("HPDS_PATH", dm.columnmeta_hpds_path);
		
		
		dt.metadata.put("study_description", dm.derived_study_description);
		dt.metadata.put("columnmeta_study_id", dm.derived_study_id.split("\\.")[0]);

		// end of compatibility
		
		// add misc metadata
		//var.getMetadata().putAll(dm.metadata);
		
		
		
		String[] dictKeyArr = entry.getKey().substring(1).split("\\\\");
		
		String dictKey = var.getStudyId() + "_" + var.getDtId();
		
		//if(dictKeyArr.length >= 2) dictKey = dictKeyArr[0].split("\\.")[0] + "_" + dictKeyArr[1].split("\\.")[0];
		
		//if(dictKeyArr.length == 1) dictKey = dictKeyArr[0].split("\\.")[0] + "_";

		//String[] varKeyArr = entry.getKey().substring(1).split("\\\\");
		String varKey = dm.derived_var_id.split("\\.")[0];

		/* replacing this varkey for compatibility
		String varKey = null;
		
		if(varKeyArr.length >= 3) varKey = varKeyArr[0].split("\\.")[0] + "_" + varKeyArr[1].split("\\.")[0] + "_" + varKeyArr[2].split("\\.")[0];
		
		if(varKeyArr.length == 2) varKey = varKeyArr[0].split("\\.")[0] + "_" + varKeyArr[1].split("\\.")[0];
		
		if(varKeyArr.length == 1) varKey = varKeyArr[0].split("\\.")[0];
		*/
		if(entry.getValue().columnmeta_data_type.equals("categorical")) {
		
			for(String value: entry.getValue().values) {
			
				var.getValues().put(value.trim(), value.trim());
			
			}
		
		}
		
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
