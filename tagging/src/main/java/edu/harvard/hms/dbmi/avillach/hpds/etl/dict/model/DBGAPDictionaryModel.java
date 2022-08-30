package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.opencsv.CSVWriter;

public class DBGAPDictionaryModel extends DictionaryModel {

	public static List<DBGAPDictionaryModel> allModels = new ArrayList<>();
	
	public static Set<String> VARIABLES_MISSING_VARIABLE_DESCRIPTION = new TreeSet<String>();

	public static Set<String> DICTIONARIES_MISSING_IN_HPDS_COLUMNMETA_DATA = new TreeSet<String>();

	private String id;
	private String name;
	private String study_id;
	private String participant_set;
	private String date_created;
	private String description;
	private List<DBGapVariable> variables = new ArrayList<>();
	
	// add a list of element filters for the dynamic metadata to ignore
	private static final List<String> ALL_ELEMENTS_FILTERS_LIST = Arrays.asList( 
	); 
	// map of element filters for the dynamic metadata to ignore ( either as a white list or black list not sure yet )
	// map is associated to the phs level this will give more granularity if elements in the list is breaking studies 
	// example black list: if ignoring "total" element breaks all other studies it should be added to this list where for the phs where we want to omit
	private static final Map<String,String>  ALL_ELEMENTS_FILTERS_MAP = new HashMap<>() {
		
	};
	
	@Override
	public Map<String, DictionaryModel> build(String[] controlFileRow, Map<String, DictionaryModel> baseDictionary) {
		
		String inputDirectory = controlFileRow[2];
		
		File studyFolder = new File(inputDirectory);
		
		System.out.println("data directory = " + inputDirectory);  
		
		if(studyFolder != null && !studyFolder.isFile()) {
			
			for(File study : studyFolder.listFiles()) {
	        	String[] fileNameArr = study.getName().split("\\.");
	        	if(fileNameArr.length < 7 ) continue; 
	        	String fileVarName = fileNameArr[fileNameArr.length - 3];
				//if(studyFolder.getName().contains("hrmn")) continue;
	        	
				if(!study.getName().endsWith("data_dict.xml")) continue;
	
				DBGAPDictionaryModel dict = new DBGAPDictionaryModel("data_dict", study.getAbsolutePath());
				
				
				if(!studyFolder.isFile()) {
					for(File varReport : studyFolder.listFiles()) {
						if(!varReport.getName().contains(fileVarName)) continue;
						if(!varReport.getName().endsWith("var_report.xml")) continue;
						
						buildVarReport(dict, varReport.getAbsolutePath());
					}
				}
				
				allModels.add(dict);
			}	
		}

		System.out.println("updating base dictionaries");

		for(DBGAPDictionaryModel model: allModels) {
			updateBaseDictionary(baseDictionary, model);
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get("/usr/local/docker-config/search/" + "DBGap_Compliant_Dictionaries_Missing_From_HPDS_Data_Store.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			
			PrintWriter pWriter = new PrintWriter(writer);
			
			for(String str: DICTIONARIES_MISSING_IN_HPDS_COLUMNMETA_DATA) {
				pWriter.println(str);
			}
	
			pWriter.flush();

			pWriter.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try(BufferedWriter writer = Files.newBufferedWriter(Paths.get("/usr/local/docker-config/search/" + "DBGap_Compliant_Dictionaries_Missing_Variable_Description.csv"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
			
			PrintWriter pWriter = new PrintWriter(writer);
			
			for(String str: VARIABLES_MISSING_VARIABLE_DESCRIPTION) {
				pWriter.println(str);
			}
			pWriter.flush();

			pWriter.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return baseDictionary;
	}

	private void updateBaseDictionary(Map<String, DictionaryModel> baseDictionary, DBGAPDictionaryModel dict) {
		dict.variables.forEach(var -> {
			
			String keyLookup = "\\" + var.study_id.split("\\.")[0] + "\\" + var.data_table_id.split("\\.")[0] + "\\" + var.variable_id.split("\\.")[0] + "\\" + var.variable_encoded_name + "\\";
			
			if(baseDictionary.containsKey(keyLookup)) {
				DictionaryModel baseModel = baseDictionary.get(keyLookup);
				
				baseModel.derived_group_description = var.data_table_description.isBlank() ? baseModel.derived_group_description: var.data_table_description;
				baseModel.derived_group_name = var.data_table_name.isBlank() ? baseModel.derived_group_name: var.data_table_name;
				baseModel.derived_group_id = var.data_table_id.isBlank() ? baseModel.derived_group_id : var.data_table_id;
				baseModel.derived_var_name = var.variable_encoded_name;
				baseModel.derived_var_id = var.variable_id.isBlank() ? baseModel.derived_var_id: var.variable_id;
				baseModel.derived_var_description = var.variable_description.isBlank() ? baseModel.derived_var_description: var.variable_description;
				baseModel.derived_study_id = var.study_id.isBlank() ? baseModel.derived_study_id : var.study_id;
				baseModel.derived_study_description = dict.description.isBlank() ? baseModel.derived_study_description : dict.description;
				
				if(baseModel.derived_var_description.isBlank()) {
					VARIABLES_MISSING_VARIABLE_DESCRIPTION.add(keyLookup);
				}
				//baseModel.metadata.putAll(dict.metadata);
			} else {
				DICTIONARIES_MISSING_IN_HPDS_COLUMNMETA_DATA.add(keyLookup);
			};
		});
		
	
		/* bad looping
		baseDictionary.entrySet().forEach(entry -> {
									
			for(DBGapVariable var: dict.variables) {
				if(entry.getValue().columnmeta_hpds_path.contains(var.study_id.split("\\.")[0]) && 
					entry.getValue().columnmeta_hpds_path.contains(var.data_table_id.split("\\.")[0]) &&
					entry.getValue().columnmeta_hpds_path.contains(var.variable_id.split("\\.")[0])) {
					
					DictionaryModel baseModel = entry.getValue();
					
					baseModel.derived_group_description = var.data_table_description.isBlank() ? baseModel.derived_group_description: var.data_table_description;
					baseModel.derived_group_name = var.data_table_name.isBlank() ? baseModel.derived_group_name: var.data_table_name;
					baseModel.derived_var_id = var.variable_id.isBlank() ? baseModel.derived_var_id: var.variable_id;
					baseModel.derived_var_description = var.variable_description.isBlank() ? baseModel.derived_var_description: var.variable_description;
					baseModel.derived_study_id = dict.study_id.isBlank() ? baseModel.derived_study_id : dict.study_id;
					baseModel.derived_study_description = dict.description.isBlank() ? baseModel.derived_study_description : dict.description;
					
				}
				
			}
		}); */
		
		
	}

	private void reportMissingColumnmeta() {
		// TODO Auto-generated method stub
		
	}

	private void buildVarReport(DBGAPDictionaryModel dict, String absolutePath) {
        File dataDictFile = new File(absolutePath);

		Document doc;
				
		try {

			doc = Jsoup.parse(dataDictFile, "UTF-8");
			
			Element dataTable = doc.getElementsByTag("data_table").first();
			
			if(dataTable != null) {
				
				dict.description = doc.getElementsByTag("data_table").first().attr("study_name");
				
				doc.getElementsByTag("variable").stream().forEach(variableElement -> {
					
					try {
						addVarReportData(dict, variableElement);
											//variables.put(variable.attr("id").replaceAll("\\.v.*", ""), dbgvar);
					}catch(Exception e) {
						e.printStackTrace();
					}
					
				});
			} else {
				System.err.println(new File(absolutePath).getName() + " dictionary does not contain a data table");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	
	}

	private void addVarReportData(DBGAPDictionaryModel dict, Element variableElement) {
		// Create a new 
		//TopmedVariable variableObject = new TopmedVariable();
	
		//variableObject.addVarReportMeta(variableElement);
		
		//Instead of using this lets use the method that already exists in the TopmedVariable class
		String varReportPrefix = "var_report_";
		
		Map<String,Integer> dictIndex = dict.buildVariableIndex();
		variableElement.getAllElements().stream().forEach(element -> {
			
			String varId = element.attr("id");
			if(varId != null || !varId.isBlank()) {
				varId = varId.split("\\.")[0];
				int indexLookup = dictIndex.containsKey(varId) ? dictIndex.get(varId) : -1;
				
				DBGapVariable dbGapVariable = indexLookup != -1 ? dict.variables.get(indexLookup) : null;
				
				if(indexLookup != -1) {
					dict.variables.get(indexLookup);
				}
				if(dbGapVariable != null) {
					dbGapVariable.all_metadata.put(varReportPrefix + "description", element.getElementsByTag("description").first().text());
					
					dbGapVariable.all_metadata.put(varReportPrefix + "study_description", dict.description);
					
					//dbGapVariable.all_metadata.putAll(collectAllMetadataForElement(varReportPrefix,element));
				}
				/*
				dict.variables.forEach(dbGapVariable ->{
					
					String[] elementVarIdArr = element.attr("id").split("\\.");
					if(elementVarIdArr.length > 0) {
						
						if(elementVarIdArr[0].startsWith("phv")) {
							String elementVarId = elementVarIdArr[0];
							// Ingest the var report into the var_report_metadata data map
							if(dbGapVariable.variable_id.startsWith(elementVarId)) {

								dbGapVariable.all_metadata.put(varReportPrefix + "description", element.getElementsByTag("description").first().text());
								
								dbGapVariable.all_metadata.put(varReportPrefix + "study_description", dict.description);
								
								dbGapVariable.all_metadata.putAll(collectAllMetadataForElement(varReportPrefix,element));

							}
						}
						
					}
					
				});*/
			}
		});
		
	}
	private Map<String, Integer> buildVariableIndex() {
		Map<String,Integer> index = new HashMap<>();
		AtomicInteger x = new AtomicInteger();  // index 
		this.variables.forEach(variable -> {
			index.put( variable.variable_id.split("\\.")[0],x.get());
			x.incrementAndGet();
		});
		return index;
	}

	/**
	 * method to gather all of a variables elements textvals and element attributes 
	 * should work with  both data_dictionaries and var report. 
	 * 
	 * @param metaDataPrefix - prefix for the metadata key that is being generated
	 * @param element - the variable element that is being ingested
	 */
	private Map<String,String> collectAllMetadataForElement(String metaDataPrefix, Element e) {
		
		Map<String,String> metadata = new HashMap<>();
		
		e.getAllElements().stream().forEach((Element element)->{
			
			
			String elementText = element.ownText();
			
			String elementParentTexts = element.parents().text();
			
			if(!ALL_ELEMENTS_FILTERS_LIST.contains(elementText)) {
				// does current element have a text val?
				// if so add to metadata
				// if multiple element tags exist create an enumerated key association
				if(!elementText.isBlank()) {
					if(metadata.containsKey(metaDataPrefix + element.tagName())) {
						int enumeratedKey = determineMetaKeyIteration(metadata.keySet(),metaDataPrefix + element.tagName());
						metadata.put(metaDataPrefix + element.tagName() + "_" + enumeratedKey, elementText);
					} else {
						metadata.put(metaDataPrefix + element.tagName(), elementText);
					}
				}
				// does current element have attributes 
				
				element.attributes().forEach((attr)->{
					if(metadata.containsKey(metaDataPrefix + attr.getKey())) {
						int enumeratedKey = determineMetaKeyIteration(metadata.keySet(),metaDataPrefix + attr.getKey());
						metadata.put(metaDataPrefix + attr.getKey() + "_" + enumeratedKey, attr.getValue());

					} else {
						metadata.put(metaDataPrefix + attr.getKey(), attr.getValue());

					}
				});
			}
		});
		
		return metadata;
	}

	private int determineMetaKeyIteration(Set<String> set, String string) {
		Set<String> matchKeys = new HashSet<>();
		set.stream().forEach(key ->{
			if(key.contains(string)) matchKeys.add(key);
		});
		
		return matchKeys.size() - 1;
	}

	public DBGAPDictionaryModel(String xmlType, String absolutePath) {
	        
        if(xmlType.equals("data_dict")) buildDataDictData(this,absolutePath);

        //TopmedDataTable topmedDataTable = new TopmedDataTable(doc, dataDictFile.getName());
	}


	private void buildDataDictData(DBGAPDictionaryModel dbgapDictionaryModel, String absolutePath) {
        File dataDictFile = new File(absolutePath);

		Document doc;
		try {

			doc = Jsoup.parse(dataDictFile, "UTF-8");
			
			id = doc.getElementsByTag("data_table").first().attr("id");
			name = dataDictFile.getName().split("\\.")[4];
			study_id = doc.getElementsByTag("data_table").first().attr("study_id");
			
			participant_set = doc.getElementsByTag("data_table").first().attr("participant_set");

			date_created = doc.getElementsByTag("data_table").first().attr("date_created");

			description = doc.getElementsByTag("data_table").first().getElementsByTag("description").first().text();

			doc.getElementsByTag("variable").stream().forEach(variable -> {
				
				DBGapVariable dbgvar;
				try {
					
					dbgvar = new DBGapVariable(this, variable);
					//dbgvar.all_metadata.putAll(collectAllMetadataForElement("data_dictionary_", variable));
					variables.add(dbgvar);
					//variables.put(variable.attr("id").replaceAll("\\.v.*", ""), dbgvar);
				}catch(Exception e) {
					e.printStackTrace();
				}
				
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	public DBGAPDictionaryModel() {
		// TODO Auto-generated constructor stub
	}

	public class DBGapVariable {
		public String study_id;
		public String variable_id;
		public String variable_encoded_name;
		public String variable_description;
		public String data_table_id;
		public String data_table_description;
		public String data_table_name;
		public String study_description;
		// hash map that will collect all non derived metadata
		// keys will be prefixed with the source of where the metadata was extracted from ( data_dictionary_ or var_report_ )
		public Map<String,String> all_metadata = new HashMap<String,String>();
		
		public DBGapVariable(DBGAPDictionaryModel dbgapDictionaryModel, Element variable) {
			study_id = dbgapDictionaryModel.study_id;
			data_table_id = dbgapDictionaryModel.id;
			data_table_description = dbgapDictionaryModel.description;
			data_table_name = dbgapDictionaryModel.name;
			variable_id = variable.id();
			variable_encoded_name = variable.getElementsByTag("name").first().text();
			variable_description = variable.getElementsByTag("description").first().text();
		}
	}

	@Override
	public Map<String, DictionaryModel> build() {
		// TODO Auto-generated method stub
		return null;
	}

}
