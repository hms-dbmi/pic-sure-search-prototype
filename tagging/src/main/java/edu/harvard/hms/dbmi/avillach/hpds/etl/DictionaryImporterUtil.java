package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvException;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.factory.DictionaryFactory;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.ColumnMetaDictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.DictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.serializer.HPDSDictionarySerializer;

/**
 * This util will ingest dictionary from data sources into readable dictionary.javabin.
 * 
 * The dictionary.javabin will contain required metadata for pic-sure-search to function.
 * 
 * 
 * @author TDeSain
 *
 */
public class DictionaryImporterUtil {
	// directory and file structure
	private static String DATA_INPUT_DIR = "./local/source/";
	private static String OUTPUT_DIR = "/usr/local/docker-config/search/";
    private static final String JAVABIN = OUTPUT_DIR + "dictionary.javabin"; 
	private static final boolean SKIP_CONTROL_FILE_HEADER = true;

	private Map<String,DictionaryModel> dictionaries = new TreeMap<>();
	private TreeMap<String, TopmedDataTable> hpdsDictionaries = new TreeMap<String, TopmedDataTable>();
	
	private Set<String> stigmatizedPaths = new HashSet<>();
	
	private List<String[]> controlFile = new ArrayList<>();
	// Index for the model schema column in the control file.
	private Integer controlFileModelTypeIndex = 3; 

	public static void main(String[] args) {
		parameterOverrides(args);
		
		try {
			
			DictionaryImporterUtil.class.getDeclaredConstructor().newInstance().run();
			
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			System.err.println("Dictionary Importer unable to complete successfully!");
			e.printStackTrace();
		}
		
	}

	/**
	 * wrapper method to execute necessary steps to build dictionary.javabin
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	private void run() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		// initialize base dictionaries from the columnMeta.csv and control file 
		buildControlFile();
		
		dictionaries = buildColumnMetaDictionaries();
		
		buildDictionaries();
		
		// Serialize dictionary models into the dictionary Data Table and Variable dictionary Objects.
		hpdsDictionaries = HPDSDictionarySerializer.class.getDeclaredConstructor().newInstance().serialize(dictionaries);

		// Stigmatizing variables			
		doStigmatizeVariables();
				
		writeDictionary();
		
	}

	private void buildControlFile() {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DictionaryFactory.DICTIONARY_CONTROL_FILE))) {
			CSVReader csvReader = new CSVReader(buffer);
			if(SKIP_CONTROL_FILE_HEADER) {
				csvReader.skip(1);
			}
			controlFile = csvReader.readAll();		
				
		} catch (IOException e) {
		    System.err.println("Unable to read control file: " + DictionaryFactory.DICTIONARY_CONTROL_FILE);
		    e.printStackTrace();
		} catch (CsvException e) {
			System.err.println("Dictionary Control File is not in csv format: " + DictionaryFactory.DICTIONARY_CONTROL_FILE);
			e.printStackTrace();
		};
		
		
	}

	/**
	 * Build method for columnMeta.csv
	 * @return
	 */
	private Map<String, DictionaryModel> buildColumnMetaDictionaries() {
		Map<String, DictionaryModel> columMetaDictionaries = new TreeMap<>();
		try {
			ColumnMetaDictionaryModel columnmetaModel = (ColumnMetaDictionaryModel) DictionaryFactory.class.getDeclaredConstructor().newInstance().getDictionaryModel("columnmetadata");
		
			columMetaDictionaries = columnmetaModel.build(controlFile, dictionaries);
			
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException e) {
			System.err.println("Error generating dictionary model.");
			e.printStackTrace();
		} 
		if(columMetaDictionaries == null || columMetaDictionaries.isEmpty()) {
			throw new RuntimeException("Error processing column meta data.");
		}
		return columMetaDictionaries;
	}


	/**
	 * Build method for control file.
	 * 
	 * A Variable must exist in columnmeta in order for them to enrich their dictionaries.
	 * If it does not exist in the columnmeta then it does not exist in the HPDS backend
	 */
	private void buildDictionaries() {
		// iterate over dict control file
		controlFile.forEach(controlFileRow -> {
			// 
			// Build current studies dictionary models and update the base dictionary object.				
			try {
				System.out.println("Processing: " + Arrays.toString(controlFileRow));

				String dictionaryModel = controlFileRow[controlFileModelTypeIndex];
				
				// This will build the appropriate model for each study based on the model schema given in the control file
				DictionaryModel controlFileModel = DictionaryFactory.class.getDeclaredConstructor().newInstance().getDictionaryModel(dictionaryModel);
				
				if(controlFileModel == null) {
					System.err.println(dictionaryModel + " is an invalid model");
				}
				// columnmeta is the base dictionaries no need to build it again.
				else if(!(controlFileModel instanceof ColumnMetaDictionaryModel)) {
					controlFileModel.build(controlFileRow, dictionaries);
				}
				
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				System.err.println("Error generating dictionary model.");
				e.printStackTrace();
			}
		});		
	}
	
	/**
     * reads in the conceptsToRemove.csv to be used for variable stigmatization.
     */
	private void readStigmatizedVariables() {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(DATA_INPUT_DIR + "conceptsToRemove.csv"))) {
			
			RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
        	
        	CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(buffer)
        			.withCSVParser(rfc4180Parser);
        	
        	CSVReader csvreader = csvReaderBuilder.build();	
        	
        	csvreader.forEach(line ->{
        		if(line[0] != null && !line[0].isBlank()) {
        			stigmatizedPaths.add(line[0]);
        		}
        		
        	});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * This methods will stigmatize variables by setting the metadata for columnmeta_is_stigmatized and is_stigmatized
	 * 
	 */
	private void doStigmatizeVariables() {
		readStigmatizedVariables();
		hpdsDictionaries.forEach((phs,variables) -> {
			variables.variables.forEach((key,variable) -> {
				String HPDS_PATH = variable.getMetadata().containsKey("columnmeta_hpds_path") ?
						variable.getMetadata().get("columnmeta_hpds_path"): "";
				
				if(HPDS_PATH.isBlank()) {
					System.err.println("HPDS_PATH MISSING FOR - " + phs + ":" + key);
				} else {
					
					if(stigmatizedPaths.contains(HPDS_PATH)) {
						variable.getMetadata().put("columnmeta_is_stigmatized", "true");
						variable.getMetadata().put("is_stigmatized", "true");
					} else {
						variable.getMetadata().put("columnmeta_is_stigmatized", "false");
						variable.getMetadata().put("is_stigmatized", "false");
					}
					
				}
				
			});
		});
	}


	/**
	 * This method will write dictionary.javabin
	 * 
	 * This method needs to be migrated to the serializer 
	 * 
	 * Currently this is a concern of mine as we are required to build a large object to store the
	 * hpdsDictionaries which is going to cause a scaling issue with the etl. - tdesain
	 * 
	 * Not sure if possible to stream the ObjectOutputStream with the current data model.
	 */
	private void writeDictionary() {
		
	    try(ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(JAVABIN)))){
	    	
	        oos.writeObject(hpdsDictionaries);
	        oos.flush();
	        
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    // code block to output dictionary in a json format
	    // very useful for development. so leaving it in.  could add a arg flag to pass that will build the json if needed.
	    // DO NOT TRY AND OUTPUT THE JSON FILE FOR THE ENTIRE DICTIONARY.  It will be a GIGANTIC file and not very useful.
	      
	    ObjectMapper mapper = new ObjectMapper();
	    
	    try {
	        System.out.println("Writing json");

			//byte[] jsonOut = mapper.writeValueAsBytes(hpdsDictionaries);
            //System.out.println("Byte array size = " + jsonOut.length);
            mapper.writeValue(new File(OUTPUT_DIR + "dictionary.json"), hpdsDictionaries);
			
		} catch (Exception e) {
            System.out.println("JSON write error");
			e.printStackTrace();
		}
	    
	    
	}


	/**
	 * Can use this method to handle making constants configurable
	 * if needed.
	 * 
	 * @param args
	 */
	private static void parameterOverrides(String[] args) {
		// TODO Auto-generated method stub
		
	}
}
