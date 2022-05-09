package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.factory.DictionaryFactory;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.ColumnMetaDictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.ColumnMetaDictionaryModel.ColumnMetaCSVRecord;

/**
 * 
 * Dictionary validations 
 * Any validations done against the dictionary should be done here.
 * 
 * @author TDeSain
 *
 */
public class DictionaryValidator {
	

	// field and metadata validation vars
	private static int COLUMNMETA_TOTAL_NUM_OF_RECORDS = 0;  // Should equal DICTIONARY_TOTAL_NUM_OF_VARIABLES
	
	private static int DICTIONARY_TOTAL_NUM_OF_VARIABLES = 0; // Should equal COLUMNMETA_TOTAL_NUM_OF_RECORDS
	
	//private static int DICTIONARY_TOTAL_NUM_OF_DATATABLES = 0; // maybe find counts of datatables undefined.  Does not mean they are invalid though as a data set might not have a defined grouping.
	
	//private static int COLUMNMETA_TOTAL_NUM_OF_RECORDS = 0;
	
	// build some objects from the dictionaries to use for validations - avoid iterating over the dictionary multiple times.
	private static Set<String> DICTIONARY_ALL_HPDS_PATHS; // used for performance to validate all paths.
	private static Set<TopmedVariable> DICTIONARY_VARIABLES_WITH_MISALIGNED_DATA_TYPES; // store the records of any misaligned data types for further debugging
	private static Set<TopmedVariable> DICTIONARY_DBGAP_MISSING_DESCRIPTION; // Does not mean a variable is invalid but may need further curation in order to detect description.
	
	// code variables.
	private static final String COLUMN_META_HPDS_PATH_KEY = "columnmeta_HPDS_PATH";

	private static final String COLUMN_META_STUDY_ID_KEY = "columnmeta_study_id";

	private static final String COLUMN_META_VAR_GROUP_ID_KEY = "columnmeta_data_group_id";

	private static final String COLUMN_META_VARIABLE_ID_KEY = "columnmeta_var_id";

	private static final Object COLUMN_META_DATA_TYPE_KEY = "columnmeta_data_type";

	private static final String COLUMN_META_MIN_KEY = "columnmeta_min";

	private static final Object COLUMN_META_MAX_KEY = "columnmeta_max";
	
	

	/**
	 * In order to validate the fields and metadata we will need the columnMeta.csv as
	 * required data is pulled from this file.
	 * 
	 * @param hpdsDictionaries
	 */
	public void validateDictionary(TreeMap<String, TopmedDataTable> hpdsDictionaries) {
		
		//Read in the columnMeta csv
        BufferedReader buffer;
        
		try {
			buffer = Files.newBufferedReader(Paths.get(DictionaryFactory.COLUMN_META_FILE));
			
	    	buildAllHpdsPaths(hpdsDictionaries);
			
	    	RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
	    	
	    	CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(buffer)
	    			.withCSVParser(rfc4180Parser)
	    			.withSkipLines(DictionaryFactory.COLUMN_META_FILE_HAS_HEADER ? 1:0);  // header implied
	    	
	    	CSVReader csvreader = csvReaderBuilder.build();
	    		    	
	    	csvreader.forEach(columnMetaCSVRecord -> {
	    		boolean isValid = true;
	    		COLUMNMETA_TOTAL_NUM_OF_RECORDS++;
	    		// validate dictionary has entry for hpds path stored in the variable
	    		String hpdsPath = columnMetaCSVRecord[0];
	    		
	    		if(!DICTIONARY_ALL_HPDS_PATHS.contains(hpdsPath)) {
	    			System.err.println("No columnmeta_HPDS_PATH found for - " + hpdsPath);
	    		} else {
	    			// variable exists in the dictionary.
	    			DICTIONARY_TOTAL_NUM_OF_VARIABLES++;
	    			// since I know that the variable exists lets go validate it.
	    			// validate all the required fields and metadata
	    			validateVariables(hpdsPath,columnMetaCSVRecord,hpdsDictionaries); // we can use the hpdsPath to make a index search against the dictionary
	    		}
	    		
	    	});
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}


	private void validateVariables(String hpdsPath, String[] columnMetaCSVRecord, TreeMap<String, TopmedDataTable> hpdsDictionaries) {
		
		String[] dataTableKeyArr = hpdsPath.split("\\\\");
		
		String dataTableKey = dataTableKeyArr.length >= 2 ? dataTableKeyArr[0] + "_" + dataTableKeyArr[1] : dataTableKeyArr[0];
		
		if(hpdsDictionaries.containsKey(dataTableKey)) {
			
			hpdsDictionaries.get(dataTableKey).variables.forEach((key,variable) ->{
				if(variable.getMetadata().get(COLUMN_META_HPDS_PATH_KEY).equals(hpdsPath)) {
					boolean isVarValid = validateVariable(variable, hpdsDictionaries.get(dataTableKey),columnMetaCSVRecord);
					
				};
			});
			
		}
		
	}


	private boolean validateVariable(TopmedVariable variable, TopmedDataTable dataTable,
			String[] columnMetaCSVRecord) {
		
		// since we have different mechanisms to build the datatable and variable objects in each model 
		// we need to use different methodologies to identify the what values are valid.
		// we can identify what the variable model type is by the root node
		// - rootnode contains phs, pht and phv values = dbgap studies, with dbgap dictionaries
		// - rootnode contains phs only = dbgap studies, with internal generated dictionaries
		// - rootnode is dcc harmonized
		// - rootnode is a generated global variable.  Each global variable will have its own validation method
		
		if(variable.getStudyId() == "phs" && variable.getDtId() == "pht" && variable.getVarId() == "pht") {
			return validateDbgapDictionary(variable,dataTable,columnMetaCSVRecord); 
		}
		
		return false; // no methodoligy detected to validate the data.  must be something wrong or we need a new case.
		
	}


	private boolean validateDbgapDictionary(TopmedVariable variable, TopmedDataTable dataTable,
			String[] columnMetaCSVRecord) {
		
		
		// check if phs , pht and phv are valid; 
		if(!variable.getStudyId().matches("phs[0-9]{6}") && variable.getMetadata().get(COLUMN_META_STUDY_ID_KEY).matches("phs[0-9]{6}")) {
			System.err.println("Variable is not a valid a dbgap study id - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
			return false;
		}
		if(!variable.getStudyId().matches("pht[0-9]{6}") && variable.getMetadata().get(COLUMN_META_VAR_GROUP_ID_KEY).matches("phs[0-9]{6}")) {
			System.err.println("Variable is not a valid a dbgap data table id - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
			return false;
		}
		if(!variable.getVarId().matches("phv[0-9]{8}") && variable.getMetadata().get(COLUMN_META_VARIABLE_ID_KEY).matches("phs[0-9]{6}")) {
			System.err.println("Variable is not a valid a dbgap data table id - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
			return false;
		}
		// run any shared validations.
		return doGeneralValidations(variable,dataTable,columnMetaCSVRecord);
	}


	private boolean doGeneralValidations(TopmedVariable variable, TopmedDataTable dataTable,
			String[] columnMetaCSVRecord) {
		boolean isValid = true;

		// checked required data
		// all variables should have at least a study id as it is tied to the root node of HPDS_PATH
		if(variable.getStudyId().isBlank() && variable.getMetadata().containsKey(COLUMN_META_STUDY_ID_KEY) && variable.getMetadata().get(COLUMN_META_STUDY_ID_KEY).isBlank()) {
			System.err.println("Variable is not a valid a dbgap study id is blank - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
			return false;
		}
		
		// data type validations
		if((variable.isIs_categorical() && variable.isIs_continuous()) || (!variable.isIs_categorical() && !variable.isIs_continuous())) {
			System.err.println("Variable data type cannot be set to same value " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
			return false;
		}
		
		ColumnMetaCSVRecord cmrecord = new ColumnMetaDictionaryModel().new ColumnMetaCSVRecord(columnMetaCSVRecord);
		
		if(!variable.getMetadata().containsKey(COLUMN_META_DATA_TYPE_KEY)) {
			System.err.println("Variable does not have a data type in the metadata - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
			return false;
		}
		if(cmrecord.isCategorical) {
			if(!variable.isIs_categorical() && !variable.getMetadata().get(COLUMN_META_DATA_TYPE_KEY).equals("categorical")) {
				System.err.println("Variable has an invalid data type - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
				return false;
			} else {
				// check for empty value list for categorical
				if(variable.getValues() == null || variable.getValues().isEmpty()) {
					System.err.println("Variable is categorical with an empty values list - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
					return false;
				}
			};
			
		} else {
			if(!variable.getMetadata().get(COLUMN_META_DATA_TYPE_KEY).equals("continuous")) {
				System.err.println("Variable has an invalid data type - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
				return false;
			} else {
				// are min and max correct?
				// currently they are saved as strings they need to validated those strings can be continuous.
				if(!NumberUtils.isCreatable(variable.getMetadata().get(COLUMN_META_MIN_KEY)) && 
						!NumberUtils.isCreatable(variable.getMetadata().get(COLUMN_META_MAX_KEY))
						){
					System.err.println("Variable has invalid max and/or min value - " + variable.getStudyId() + "_" + variable.getDtId() + "_" + variable.getVarId());
					return false;
				}
			};
		}
		
		return isValid;
		
	}


	private void buildAllHpdsPaths(TreeMap<String, TopmedDataTable> hpdsDictionaries) {
		
		hpdsDictionaries.entrySet().parallelStream().forEach(entry -> {
			
			entry.getValue().variables.forEach((k, var) ->{
			
				if(!var.getMetadata().containsKey("COLUMN_META_HPDS_PATH_KEY")) System.err.println("columnmeta_HPDS_PATH MISSING! - " + var.getStudyId() + "." + var.getVarId());
				
				else DICTIONARY_ALL_HPDS_PATHS.add(var.getMetadata().get(COLUMN_META_HPDS_PATH_KEY));
				
				
				
			});
			
		});
		
	}

	
}
