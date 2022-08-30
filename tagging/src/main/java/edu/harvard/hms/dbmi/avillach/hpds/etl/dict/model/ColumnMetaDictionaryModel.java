package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.math.NumberUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.factory.DictionaryFactory;

/**
 * Model for the columnMeta.csv
 * This is the backbone of the dictionary integration as it is responsible for storing the 
 * HPDS_PATH metadata that is used to link the data dictionary data store to the hpds data store.
 * 
 * All variables that are accessible in authorized hpds columnMeta.javabin should have a model generated for them.
 * 
 */
public class ColumnMetaDictionaryModel extends DictionaryModel {
	
	// object to store each record in the columnmeta.csv file.
    public class ColumnMetaCSVRecord {
    	public String name;
    	public int widthInBytes;
    	public int columnOffset;
    	public boolean isCategorical;
    	public List<String> categoryValues;
    	public Double min, max;
    	public long allObservationsOffset;
    	public long allObservationsLength;
    	public int observationCount;
    	public int patientCount;
    	public Map<String, String> valuesMap;
    	public List<String> values;
    	
    	/**
    	 * Constructor to build a object from a the csv buffer.
    	 * @param metaRecord
    	 */
    	public ColumnMetaCSVRecord(String[] metaRecord) {
			//System.out.println(Arrays.toString(metaRecord));
			this.name = metaRecord[0];
			this.widthInBytes = NumberUtils.isCreatable(metaRecord[1]) ? Integer.valueOf(metaRecord[1]): null;
			this.columnOffset = NumberUtils.isCreatable(metaRecord[2]) ? Integer.valueOf(metaRecord[2]): null;
			this.isCategorical = Boolean.parseBoolean(metaRecord[3]);
			this.categoryValues = isCategorical ? Arrays.asList(metaRecord[4].split("µ")) : null;
			this.min =  NumberUtils.isCreatable(metaRecord[5]) ? Double.valueOf(metaRecord[5]): null;
			this.max = NumberUtils.isCreatable(metaRecord[6]) ? Double.valueOf(metaRecord[6]): null;
			this.allObservationsOffset = NumberUtils.isCreatable(metaRecord[7]) ? Long.valueOf(metaRecord[7]): null;
			this.allObservationsLength = NumberUtils.isCreatable(metaRecord[8]) ? Long.valueOf(metaRecord[8]): null;
			this.observationCount = NumberUtils.isCreatable(metaRecord[9]) ? Integer.valueOf(metaRecord[9]): null;
			this.patientCount = NumberUtils.isCreatable(metaRecord[10]) ? Integer.valueOf(metaRecord[10]): null;

			this.values = Arrays.asList(metaRecord[4].split("µ"));
			
		}	
    	
    }
	
    /**
     * This constructor will build a dictionary model for each record.
     * 
     * We can use the control file to pull abbreviations or use the metadata.json file from the 
     * hpds data ingestion.  Currently the control file is required for each study to have an entry so
     * is safe to use.
     * 
     * All required fields need to be set by the columnMeta.csv as it is the only source of
     * truth that we know has to exist for each variable.  This constructor in tandem with the build method is responsible 
     * for setting these values.
     * 
     * @param controlFile
     * @param columnMetaRecord
     */
	private ColumnMetaDictionaryModel(List<String[]> controlFile, String[] columnMetaRecord) {
		super();
		// hashing hpds path for unique front end indentifiers without backslashes
		// hpds path is the unique key for each variable hashing this will give unique id
		// minus any hash collisions
		ColumnMetaCSVRecord cmr = new ColumnMetaCSVRecord(columnMetaRecord);
		
		String[] hpdsNodes =  cmr.name.substring(1, cmr.name.length() - 1).split("\\\\");
				
		this.hashed_var_id = DictionaryFactory.hashVarId(cmr.name);
		
		// encoded variable is always last node of hpds_path
		this.derived_var_name = hpdsNodes[hpdsNodes.length - 1];
		
		this.derived_var_id = hpdsNodes.length > 3 ? hpdsNodes[hpdsNodes.length - 2]: this.derived_var_name; // if it is four nodes expected it to phv? Potentially bad

		this.derived_var_description = ""; 
		
		// Can look for pht here at least 
		this.derived_group_id = findDerivedGroupId(hpdsNodes); 
		
		this.derived_group_name = "";		
		
		this.derived_group_description = "";
		
		this.derived_study_id = hpdsNodes[0];
		
		this.derived_study_description = "";
		
		this.derived_study_abv_name = findStudAbvName(controlFile,this.derived_study_id);
		
		this.columnmeta_data_type = cmr.isCategorical ? "categorical": "continuous";
		
		this.columnmeta_is_stigmatized = "false";  // needs to be updated by a stigmatizing methodology
		
		this.columnmeta_min = String.valueOf(cmr.min);
		
		this.columnmeta_max = String.valueOf(cmr.max);
		
		this.columnmeta_observation_count = String.valueOf(cmr.observationCount);
		
		this.columnmeta_patient_count =  String.valueOf(cmr.patientCount);
		
		this.columnmeta_hpds_path = cmr.name;
		
		this.values = cmr.values;
				
		
	}
	
	/**
	 * find the study abv name and return it's value from the control file
	 * @param controlFile
	 * @param derived_study_id
	 * @return
	 */
	private String findStudAbvName(List<String[]> controlFile, String derived_study_id) {
		for(String[] controlFileRec: controlFile) {
			if(controlFileRec[1].equals(derived_study_id)) return controlFileRec[0];
		}
		return "";
	}
	
	/**
	 * This will attempt to find the data table id ( aka group id ) from the concept path.
	 * This is currently safe to do as the pht for compliant dbgap studies exists in the concept path
	 * 
	 * If this information is removed from the concept path then this methodology will need to change.
	 * 
	 * Non-Compliant dbgap studies are handled in the DefaultJsonDictionaryModel as they do not store 
	 * data table id's in concept paths.
	 * 
	 * @param hpdsNodes
	 * @return
	 */
	private String findDerivedGroupId(String[] hpdsNodes) {
		for(String node: hpdsNodes) {
			if(node.matches("pht[0-9]{6}")) {
				return node;
			}
		}
		// no group id found 
		return "";
	}
	
	/**
	 * 
	 */
	@Override
	public Map<String, DictionaryModel> build() {
		// read columnmeta csv 
        BufferedReader buffer;
        Map<String, DictionaryModel> dictionaries = new TreeMap<>();
        
		try {
			buffer = Files.newBufferedReader(Paths.get(DictionaryFactory.COLUMN_META_FILE));
		
	    	
	    	RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
	    	
	    	CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(buffer)
	    			.withCSVParser(rfc4180Parser)
	    			.withSkipLines(DictionaryFactory.COLUMN_META_FILE_HAS_HEADER ? 1:0);  // header implied
	    	
	    	CSVReader csvreader = csvReaderBuilder.build();
	    	
	    	csvreader.forEach(columnMetaCSVRecord -> {
	    		
	    		String hpdsPath = columnMetaCSVRecord[0];
	    		
	    		dictionaries.put(hpdsPath,new ColumnMetaDictionaryModel(null, columnMetaCSVRecord));
	    		
	    	});
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dictionaries;
        
	}

	/** 
	 * Columnmeta is the base dictionary generated which this method is meant to do
	 * could be used if another base dictionary is used instead.
	 */
	@Override
	public Map<String, DictionaryModel> build(Map<String, DictionaryModel> baseDictionary) {
		// TODO Auto-generated method stub
		return null;
	}

	public ColumnMetaDictionaryModel() {
		super();
		// TODO Auto-generated constructor stub
	}
	@Override
	public Map<String, DictionaryModel> build(String[] controlFileRow, Map<String, DictionaryModel> baseDictionary) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Map<String, DictionaryModel> build(List<String[]> controlFile,
			Map<String, DictionaryModel> dictionaries) {
	
        BufferedReader buffer;
        
		try {
			buffer = Files.newBufferedReader(Paths.get(DictionaryFactory.COLUMN_META_FILE));
		
	    	
	    	RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
	    	
	    	CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(buffer)
	    			.withCSVParser(rfc4180Parser)
	    			.withSkipLines(DictionaryFactory.COLUMN_META_FILE_HAS_HEADER ? 1:0);  // header implied
	    	
	    	CSVReader csvreader = csvReaderBuilder.build();
	    	
	    	csvreader.forEach(columnMetaCSVRecord -> {
	    		
	    		String hpdsPath = columnMetaCSVRecord[0];
	    		
	    		dictionaries.put(hpdsPath,new ColumnMetaDictionaryModel(controlFile, columnMetaCSVRecord));
	    		
	    	});
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dictionaries;
	}

}
