package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.factory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.common.hash.Hashing;

import edu.harvard.hms.dbmi.avillach.hpds.etl.DBGAPDictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.DCCHarmonizedDictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.ColumnMetaDictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.DefaultJsonDictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.DictionaryModel;

/**
 * Factory will read from the input directory and use the control file to handle with dictionary
 * model to build.
 * 
 * All models will return a collection<String,DictionaryModel>
 * 
 * Factory will also call a conversion method to convert models to create the objects for TopmedDataTable 
 * and TopmedVariable
 *  
 * @author TDeSain
 *
 */
public class DictionaryFactory {
		
	public static boolean COLUMN_META_FILE_HAS_HEADER = false;
	/**
	 *  External Config / Required files
	 */
	public static String CONFIG_DIR = "./configs/";
	// Config file to control study and its dictionary builder to use
	public static String DICTIONARY_CONTROL_FILE = CONFIG_DIR + "dictionary_control_file.csv";
	// columnmeta data file.  required to generate any dictionaries
	private static String DATA_INPUT_DIR = "/local/source/";

	public static String COLUMN_META_FILE = DATA_INPUT_DIR + "columnMeta.csv";

	/**
	 * get Directory model to use to build base dictionaries
	 */

	public DictionaryModel getDictionaryModel(String dictionaryModel) {
		if(dictionaryModel.equalsIgnoreCase("columnmetadata")) {
			return new ColumnMetaDictionaryModel();
		} 
		if(dictionaryModel.equalsIgnoreCase("DefaultJsonDictionaryModel")) {
			return new DefaultJsonDictionaryModel();
		}
		if(dictionaryModel.equalsIgnoreCase("DBGAPDictionaryModel")) {
			return new DBGAPDictionaryModel();
		}
		if(dictionaryModel.equalsIgnoreCase("DCCHarmonizedDictionaryModel")) {
			return new DCCHarmonizedDictionaryModel();
		}
		return null;
	}

	public static boolean validate() {
		if(!Files.exists(Paths.get(DictionaryFactory.CONFIG_DIR))) throw new RuntimeException(DictionaryFactory.CONFIG_DIR + " does not exist");
		if(!Paths.get(DictionaryFactory.CONFIG_DIR).toFile().isDirectory()) throw new RuntimeException(DictionaryFactory.CONFIG_DIR + " is not a directory");

		if(!Files.exists(Paths.get(DictionaryFactory.DICTIONARY_CONTROL_FILE))) throw new RuntimeException(DictionaryFactory.DICTIONARY_CONTROL_FILE + " does not exist");
		if(!Files.exists(Paths.get(DictionaryFactory.COLUMN_META_FILE))) throw new RuntimeException(COLUMN_META_FILE + " does not exist");

		return true;
	}

	public static String hashVarId(String hpdsPath) {
		
		return Hashing.sha256().hashString(hpdsPath, StandardCharsets.UTF_8)
				  .toString();
		
	}
	
}
