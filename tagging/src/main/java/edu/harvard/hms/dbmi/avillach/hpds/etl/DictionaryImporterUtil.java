package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvException;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.factory.DictionaryFactory;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.ColumnMetaDictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.DictionaryModel;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model.HPDSDictionarySerializer;

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
	private static String DATA_INPUT_DIR = "/local/source/";
	private static String OUTPUT_DIR = "/usr/local/docker-config/search/";
    private static final String JAVABIN = OUTPUT_DIR + "dictionary.javabin"; //"/usr/local/docker-config/search/dictionary.javabin";

	private Map<String,DictionaryModel> dictionaries = new TreeMap<>();
	private TreeMap<String, TopmedDataTable> hpdsDictionaries = new TreeMap<String, TopmedDataTable>();
	
	private Set<String> stigmatizedPaths = new HashSet<>();
    
	public static void main(String[] args) {
		parameterOverrides(args);
		
		//preRunValidations();
		// Build base dictionary entries 
		
		try {
			
			DictionaryImporterUtil.class.getDeclaredConstructor().newInstance().run();
			
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			System.err.println("Dictionary Importer unable to complete successfully!");
			e.printStackTrace();
		}
		
	}


	private void run() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		dictionaries = buildColumnMetaDictionaries();

		buildDictionariesFromControlFile();		
		
		hpdsDictionaries = HPDSDictionarySerializer.class.getDeclaredConstructor().newInstance().serialize(dictionaries);
			
		readStigmatizedVariables();
		
		doStigmatizeVariables();
		
		//validateDictionary();
		
		writeDictionary();
		System.out.println("test");
		System.out.println("test");
		System.out.println("test");
		System.out.println("test");
		System.out.println("test");
	}

	/**
	 * Validations need to be done to ensure the following:
	 * - Required fields contain their expected values 
	 * - Required metadata contain their expected values
	 * - Check that each record in the columnMeta.csv has had a dictionary generated for it.
	 * 
	 */
	private void validateDictionary() {
		
		DictionaryValidator validator = new DictionaryValidator();
		
		validator.validateDictionary(hpdsDictionaries);
		
		
	}


	private Map<String, DictionaryModel> buildColumnMetaDictionaries() {
		try {
			ColumnMetaDictionaryModel columnmetaModel = (ColumnMetaDictionaryModel) DictionaryFactory.class.getDeclaredConstructor().newInstance().getDictionaryModel("columnmetadata");
			
			BufferedReader buffer = Files.newBufferedReader(Paths.get(DictionaryFactory.DICTIONARY_CONTROL_FILE));
			
			CSVReader csvReader = new CSVReader(buffer);
			
			List<String[]> controlFile = csvReader.readAll();
			
			csvReader.close();
			
			return columnmetaModel.build(controlFile, dictionaries);
			
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException e) {
			System.err.println("Error generating dictionary model.");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}


	private void buildDictionariesFromControlFile() {
		
		try {
			// iterate over dict control file
			BufferedReader buffer = Files.newBufferedReader(Paths.get(DictionaryFactory.DICTIONARY_CONTROL_FILE));
			
			CSVReader csvReader = new CSVReader(buffer);
			
			csvReader.readAll().forEach(controlFileRow -> {
				// variables must exist in columnmeta.
				// Build current studies dictionary models and update the base dictionary object.				
				try {
					System.out.println(Arrays.toString(controlFileRow));

					String dictionaryModel = controlFileRow[3];
					
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
		} catch (IOException | CsvException e) {
			System.err.println("Error Reading Dictionary control file.");
			e.printStackTrace();
		}
		
		
	}


	private static void preRunValidations() {
		// validate required files and directory structure exist
		if(!DictionaryFactory.validate()) throw new RuntimeException("Dictionary config invalid"); 
		if(!Files.exists(Paths.get(DATA_INPUT_DIR))) throw new RuntimeException(DATA_INPUT_DIR + " does not exist");
		if(!Paths.get(DATA_INPUT_DIR).toFile().isDirectory()) throw new RuntimeException(DATA_INPUT_DIR + " is not a directory");
		
		if(!Files.exists(Paths.get(OUTPUT_DIR))) throw new RuntimeException(OUTPUT_DIR + " does not exist");
		if(!Paths.get(OUTPUT_DIR).toFile().isDirectory()) throw new RuntimeException(OUTPUT_DIR + " is not a directory");

		//if(!Files.exists(Paths.get(DICTIONARY_OUTPUT_NAME))) throw new RuntimeException(DICTIONARY_OUTPUT_NAME + " does not exist");
		//if(!Paths.get(DICTIONARY_OUTPUT_NAME).toFile().isDirectory()) throw new RuntimeException(DICTIONARY_OUTPUT_NAME + " is not a directory");

	}

    private void writeDictionary() {
    	
        try(ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(JAVABIN)))){
        	
            oos.writeObject(hpdsDictionaries);
            oos.flush();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        ObjectMapper mapper = new ObjectMapper();
        
        try {
			String jsonOut = mapper.writeValueAsString(hpdsDictionaries);
			
			Files.write(Paths.get(OUTPUT_DIR + "dictionary.json"), jsonOut.getBytes());
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
    
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
	
	private static void parameterOverrides(String[] args) {
		// TODO Auto-generated method stub
		
	}

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
}
