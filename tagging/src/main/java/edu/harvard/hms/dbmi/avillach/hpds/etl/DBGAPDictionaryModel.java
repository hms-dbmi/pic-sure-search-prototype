package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.DictionaryModel;

public class DBGAPDictionaryModel extends DictionaryModel {

	public static List<DBGAPDictionaryModel> allModels = new ArrayList<>();
	
	private String id;
	private String name;
	private String study_id;
	private String participant_set;
	private String date_created;
	private String description;
	private Set<DBGapVariable> variables = new HashSet<>();
	
	
	@Override
	public Map<String, DictionaryModel> build(String[] controlFileRow, Map<String, DictionaryModel> baseDictionary) {
		
		String inputDirectory = controlFileRow[2];
		
		File studyFolder = new File(inputDirectory);
		
		System.out.println(inputDirectory);  //debug
		
		if(studyFolder != null && !studyFolder.isFile()) {
			
			for(File study : studyFolder.listFiles()) {
	        	String[] fileNameArr = study.getName().split("\\.");
	        	if(fileNameArr.length < 7 ) continue; 
	        	String fileVarName = fileNameArr[fileNameArr.length - 3];
				//if(studyFolder.getName().contains("hrmn")) continue;
	        	
				if(!study.getName().endsWith("data_dict.xml")) continue;
	
				DBGAPDictionaryModel dict = new DBGAPDictionaryModel("data_dict", study.getAbsolutePath());
				
				// add var data 
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
		List<DBGAPDictionaryModel> models = allModels;
		
		for(DBGAPDictionaryModel model: allModels) {
			updateBaseDictionary(baseDictionary, model);
		}
		
		return baseDictionary;
	}

	private void buildVarReport(DBGAPDictionaryModel dict, String absolutePath) {
        File dataDictFile = new File(absolutePath);

		Document doc;	
		
		try {

			doc = Jsoup.parse(dataDictFile, "UTF-8");
			
			dict.description = doc.getElementsByTag("data_table").first().attr("study_name");

			doc.getElementsByTag("variable").stream().forEach(variableElement -> {
				
				DBGapVariable dbgvar;
				try {
					addVarReportData(dict, variableElement);
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

	private void addVarReportData(DBGAPDictionaryModel dict, Element variableElement) {
		
		variableElement.getAllElements().stream().forEach(element -> {
			
			Element varDescriptionElement = element.getElementsByTag("description").first();
			if(varDescriptionElement!=null) {
				dict.variables.forEach(dbGapVariable ->{
					
					String[] elementVarIdArr = element.attr("id").split("\\.");
					if(elementVarIdArr.length > 0) {
						
						if(elementVarIdArr[0].startsWith("phv")) {
							String elementVarId = elementVarIdArr[0];
							
							if(dbGapVariable.variable_id.startsWith(elementVarId)) {
								dbGapVariable.variable_description = element.getElementsByTag("description").first().text();
							}
						}
						
					}
					
				});
			}
		});
		
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
		public String variable_description;
		public String data_table_id;
		public String data_table_description;
		public String data_table_name;
		
		public DBGapVariable(DBGAPDictionaryModel dbgapDictionaryModel, Element variable) {
			study_id = dbgapDictionaryModel.study_id;
			data_table_id = dbgapDictionaryModel.id;
			data_table_description = dbgapDictionaryModel.description;
			data_table_name = dbgapDictionaryModel.name;
			variable_id = variable.id();
		}
	}

	@Override
	public Map<String, DictionaryModel> build() {
		// TODO Auto-generated method stub
		return null;
	}

	private void updateBaseDictionary(Map<String, DictionaryModel> baseDictionary, DBGAPDictionaryModel dict) {
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
		}); 
		
		
	}

}
