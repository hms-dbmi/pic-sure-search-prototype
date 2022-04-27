package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;

public class HPDSDictionarySerializer {
	
	private static TreeMap<String, TopmedDataTable> hpdsDictionary = new  TreeMap<String, TopmedDataTable>();
	
	public static TreeMap<String, TopmedDataTable> serialize(Map<String, DictionaryModel> dictionaries) {
		for(Entry<String,DictionaryModel> entry: dictionaries.entrySet()) {

			buildDataTable(entry);
		}
		// build tags
		buildTags();
		return hpdsDictionary;
	}

	private static void buildTags() {
		for(Entry<String, TopmedDataTable> hpdsDictEntry: hpdsDictionary.entrySet()) {
			
			hpdsDictEntry.getValue().variables.forEach((varid, var) -> {
				var.getMetadata().forEach((k,v) -> {
					try {
					
						var.getMetadata_tags().addAll(TopmedVariable.class.getDeclaredConstructor().newInstance().filterTags(v));
					
						for(String value: var.getValues().values()) {
							var.getValue_tags().addAll(TopmedVariable.class.getDeclaredConstructor().newInstance().filterTags(value));
				
						}
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
		var.setVarId(dm.derived_var_id);
		var.setDtId(dm.derived_group_id);
		var.setIs_categorical(dm.columnmeta_data_type.equals("categorical"));
		var.setIs_continuous(dm.columnmeta_data_type.equals("continous"));
		var.setStudyId(dm.derived_study_id);
		if(dm.columnmeta_data_type.equals("continous")) {
			var.getMetadata().put("min", dm.getColumnmeta_min());
			var.getMetadata().put("max", dm.getColumnmeta_max());
		}
		//var.getMetadata().putAll(dm.metadata);
		
		String[] dictKeyArr = entry.getKey().substring(1).split("\\\\");
		
		String dictKey = null;
		
		if(dictKeyArr.length >= 2) dictKey = dictKeyArr[0] + "_" + dictKeyArr[1];
		
		if(dictKeyArr.length == 1) dictKey = dictKeyArr[0];

		String[] varKeyArr = entry.getKey().substring(1).split("\\\\");
		
		String varKey = null;
		
		if(varKeyArr.length >= 3) varKey = varKeyArr[0] + "_" + varKeyArr[1] + "_" + varKeyArr[2];
		
		if(varKeyArr.length == 2) varKey = varKeyArr[0] + "_" + varKeyArr[1];
		
		if(varKeyArr.length == 1) varKey = varKeyArr[0];
		
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