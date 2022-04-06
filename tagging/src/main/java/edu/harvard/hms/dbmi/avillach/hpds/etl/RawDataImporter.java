package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.math.NumberUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import edu.harvard.hms.dbmi.avillach.hpds.etl.metadata.model.DefaultJsonDataDictionaryBuilder;

public class RawDataImporter {
    
	private static String outputDirectory = "/usr/local/docker-config/search/";

    private static final String JAVABIN = outputDirectory + "dictionary.javabin"; //"/usr/local/docker-config/search/dictionary.javabin";
    private TreeMap<String, TopmedDataTable> fhsDictionary;
    private String inputDirectory;
    private TreeMap<String, TopmedDataTable> columnMetaDictionary;
    private TreeMap<String, TopmedDataTable> jsonMetaDictionary;
    private TreeMap<String, String> harmonizedMetaDictionary;

    private Set<String> stigmatizedPaths = new HashSet<>();
    
    public RawDataImporter(String inputDirectory) {

    	this.inputDirectory = inputDirectory;

    }

    public class ColumnMetaCSVRecord {
    	public String name;
    	public int widthInBytes;
    	public int columnOffset;
    	public boolean categorical;
    	public List<String> categoryValues;
    	public Double min, max;
    	public long allObservationsOffset;
    	public long allObservationsLength;
    	public int observationCount;
    	public int patientCount;
    	
		public ColumnMetaCSVRecord(String[] metaRecord) {
			
			this.name = metaRecord[0];
			this.widthInBytes = NumberUtils.isCreatable(metaRecord[1]) ? Integer.valueOf(metaRecord[1]): null;
			this.columnOffset = NumberUtils.isCreatable(metaRecord[2]) ? Integer.valueOf(metaRecord[2]): null;
			this.categorical = Boolean.parseBoolean(metaRecord[3]);
			this.categoryValues = categorical ? Arrays.asList(metaRecord[4].substring(1,metaRecord[4].length()-1).split(",")) : null;
			this.min =  NumberUtils.isCreatable(metaRecord[5]) ? Double.valueOf(metaRecord[5]): null;
			this.max = NumberUtils.isCreatable(metaRecord[6]) ? Double.valueOf(metaRecord[6]): null;
			this.allObservationsOffset = NumberUtils.isCreatable(metaRecord[7]) ? Long.valueOf(metaRecord[7]): null;
			this.allObservationsLength = NumberUtils.isCreatable(metaRecord[8]) ? Long.valueOf(metaRecord[8]): null;
			this.observationCount = NumberUtils.isCreatable(metaRecord[9]) ? Integer.valueOf(metaRecord[9]): null;
			this.patientCount = NumberUtils.isCreatable(metaRecord[10]) ? Integer.valueOf(metaRecord[10]): null;

		
		}
    	
    	
    	
    }
	public void run() throws IOException {
        fhsDictionary = new TreeMap<>();
        columnMetaDictionary = new TreeMap<>();
        jsonMetaDictionary  = new TreeMap<>();
        harmonizedMetaDictionary  = new TreeMap<>();
        
        System.out.println(inputDirectory);
        for(File studyFolder : new File(inputDirectory).listFiles()) {
        	if(studyFolder.isFile()) continue;
            if(studyFolder!=null) {
            	// needs to be changed to be for any xml
            	if(Arrays.stream(new File(studyFolder, "rawData")
                    .list((file, name)->{
                        return name.endsWith("data_dict.xml");}
                    )) == null) continue;
                    
            	Arrays.stream(new File(studyFolder, "rawData")
                    .list((file, name)->{
                        return name.endsWith("data_dict.xml");}
                    )).forEach((table)->{
                TopmedDataTable topmedDataTable;
                try {
                    topmedDataTable = loadDataTable(studyFolder.getAbsolutePath()+"/rawData/"+table);
                    fhsDictionary.put(topmedDataTable.metadata.get("study_id").replaceAll("\\.v.*", "") + "_" + topmedDataTable.metadata.get("id").replaceAll("\\.v.*", ""), topmedDataTable);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                });
            	
            }
        }

        for(File studyFolder : new File(inputDirectory).listFiles()) {
        	if(studyFolder.isFile()) continue;

            Arrays.stream(new File(studyFolder, "rawData").list((file, name)->{
                return name.endsWith("var_report.xml");}
            )).forEach((table)->{
                String[] split = table.split("\\.");
                if(split.length > 2) {
                    String tableId = split[0] + "_" + split[2];
                    TopmedDataTable topmedDataTable = fhsDictionary.get(tableId);
                    if(topmedDataTable!=null) {
                        try {
                            Document doc = Jsoup.parse(new File(studyFolder.getAbsolutePath()+"/rawData/"+table), "UTF-8");
                            topmedDataTable.loadVarReport(doc);
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        TreeSet<String> tags = new TreeSet<>();
        
        buildJsonMetaDictionary();

        buildHarmonizedMetaDictionary();
        
        
        System.out.println("###-Syncing values to column metadata-###");
        long columnMetaRecCount = 0;
    	ConcurrentSkipListSet<String> nonIngestedMetaRecords = new ConcurrentSkipListSet<String>();

        try(BufferedReader buffer = Files.newBufferedReader(Paths.get(inputDirectory + "/columnMeta.csv"))) {
        	
        	RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
        	
        	CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(buffer)
        			.withCSVParser(rfc4180Parser);
        	
        	CSVReader csvreader = csvReaderBuilder.build();
        	csvreader.forEach(columnMetaCSVRecord -> {
        		
        		ColumnMetaCSVRecord csvr = new ColumnMetaCSVRecord(columnMetaCSVRecord);
        		String[] concept = csvr.name.substring(1,csvr.name.length() - 1).split("\\\\");
        		String dt = null; 
        		int studyDepth = concept.length;
        		
        		if(studyDepth == 4) {
        			dt = concept[0] + "_" + concept[1];
        		}
        		if(studyDepth == 3) {
        			dt = concept[0] + "_" + concept[1];
        		}
        		if(studyDepth == 2) {
        			dt = concept[0] + "_" + concept[1];
        		}
        		if(studyDepth == 1) {
        			dt = concept[0];
        		}
        		if(concept[0].equals("_studies_consents")) {
        			if(studyDepth == 3) {
            			dt = concept[0] + "_" + concept[1] + "_" + concept[2];
            		}
            		if(studyDepth == 2) {
            			dt = concept[0] + "_" + concept[1];
            		}
            		if(studyDepth == 1) {
            			dt = concept[0];
            		}
        		}
        		if(dt != null) {
        			if(columnMetaDictionary.containsKey(dt)) {
        				TopmedVariable var =  new TopmedVariable(columnMetaDictionary.get(dt), csvr);
        				
        				if(columnMetaDictionary.get(dt).variables.containsKey(var.getVarId())) {
        				
        					nonIngestedMetaRecords.add(var.getStudyId() + ":" + dt + ":" + var.getVarId());
        					
        				} else {
        					columnMetaDictionary.get(dt).variables.put(var.getVarId(), var);
        				}
        			} else {
        				columnMetaDictionary.put(dt, new TopmedDataTable(csvr));
        			}
        		} else {
        			nonIngestedMetaRecords.add(csvr.name);
        			System.err.println("Dictionary not created for=" + csvr.name);
        		}
        	});
        	
        	columnMetaRecCount = csvreader.getLinesRead();
        }
        
        
        for(TopmedDataTable table : fhsDictionary.values()) {
            Collection<TopmedVariable> variables = table.variables.values();
            table.generateTagMap();
            
            for(TopmedVariable variable : variables) {
            	//variable.getMetadata().put("HPDS_PATH", buildVariableConceptPath(variable));
                tags.addAll(variable.getMetadata_tags());
                tags.addAll(variable.getValue_tags());
            }
        }
        // merge xml dictionaries to column metadata
        mergeDictionaries();
        
        stigmatizedVariables();
        
        buildVarTags();
        
        writeDictionary();
        
        TreeMap<String, TopmedDataTable> dictionary = readDictionary();
        
        AtomicInteger dictionaryTotalVars = new AtomicInteger();
        
        Set<String> dictKS = new HashSet<String>();
        Set<String> cmdKS = new HashSet<String>();
        columnMetaDictionary.keySet().forEach(key -> {
        	columnMetaDictionary.get(key).variables.keySet().forEach(k -> {
            	cmdKS.add(key + ":" + k);

        	});
        });
        
        dictionary.keySet().forEach(key -> {
        	dictionary.get(key).variables.keySet().forEach(k -> {
        		dictKS.add(key + ":" + k);

        	});        
        });
        
        Set<String> allHpdsPaths = new HashSet<String>(); 
        dictionary.keySet().forEach(key -> { 
        	if(dictionary.get(key).variables.values().isEmpty()) {
        		nonIngestedMetaRecords.add(dictionary.get(key).metadata.get("id") + " - " + dictionary.get(key).metadata.get("study_id"));
        	}
        	if(!dictionary.get(key).metadata.containsKey("columnmeta_description")) {
        		System.err.println("Dictionary table does not have a description=" + dictionary.get(key));
        		nonIngestedMetaRecords.add(dictionary.get(key) + " - Data Table missing Description");
        	}
        	if(dictionary.get(key).metadata.containsKey("columnmeta_description") && dictionary.get(key).metadata.get("columnmeta_description").isBlank()) {

        		System.err.println("Dictionary table description is blank=" + dictionary.get(key).metadata.get("id"));
        		nonIngestedMetaRecords.add(dictionary.get(key) + " - Data Table blank Description");
        	}
        	dictionary.get(key).variables.values().forEach((TopmedVariable value) -> {
        		
        		dictionaryTotalVars.getAndIncrement();
        		
        		if(!value.getMetadata().containsKey("columnmeta_description")) {
        			System.err.println("Dictionary description for variable is missing=" + value.getStudyId() + " - " + value.getVarId());
        			nonIngestedMetaRecords.add(dictionary.get(key) + " - Dictionary description for variable is missing=" + value.getStudyId() + " - " + value.getVarId());
        		}
        		if(value.getMetadata().containsKey("columnmeta_description") && value.getMetadata().get("columnmeta_description").isBlank()) {
        			System.err.println("Dictionary description for variable is blank=" + value.getStudyId() + " - " + value.getVarId());
        			nonIngestedMetaRecords.add(dictionary.get(key) + " - Dictionary description for variable is blank=" + value.getStudyId() + " - " + value.getVarId());
        		}
        		if(!value.getMetadata().containsKey("columnmeta_HPDS_PATH")) {
        			System.err.println("Dictionary variable missing columnmeta_HPDS_PATH=" + value.getStudyId() + " - " + value.getVarId());
        		} else {
        			if(allHpdsPaths.contains(value.getMetadata().get("columnmeta_HPDS_PATH"))) {
        				System.err.println("HPDS_PATH exists in multiple locations: " + value.getMetadata().get("columnmeta_HPDS_PATH"));
        			} else {
            			allHpdsPaths.add(value.getMetadata().get("columnmeta_HPDS_PATH"));
        			}
        		}
        	    if (value.getMetadata().get("columnmeta_description").equals(value.getMetadata().get("columnmeta_name"))) {
        			System.err.println("Dictionary variable name equals description - " + value.getStudyId() + ":" + value.getVarId());
        		}
        	});
        	
        	/* this method call is no longer valid as non dbgap studies are not 4 level of concept depth
        	 * each dictionary variable has an hpds_path saved in it's metadata will display that instead
        	 * 
            dictionary.get(key).variables.values().forEach((TopmedVariable value) -> { 
	        	System.out.println(buildVariableConceptPath(value));
            });*/
            
        });  
        HashSet<String> pathsFound = new HashSet<>();
        try(BufferedReader buffer = Files.newBufferedReader(Paths.get(inputDirectory + "/columnMeta.csv"))) {
        	
        	RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
        	
        	CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(buffer)
        			.withCSVParser(rfc4180Parser);
        	
        	CSVReader csvreader = csvReaderBuilder.build();
        	csvreader.forEach(columnMetaCSVRecord -> {
        		
        		ColumnMetaCSVRecord csvr = new ColumnMetaCSVRecord(columnMetaCSVRecord);
        		String concept = csvr.name;
        		if(!allHpdsPaths.contains(csvr.name)) {
        			nonIngestedMetaRecords.add("columnMeta not ingested: " + csvr.name);
        		} 
        		if(!pathsFound.contains(csvr.name)) {
        			pathsFound.add("csvr.name");
        		} else {
        			nonIngestedMetaRecords.add("columnMeta path has multiple records: " + csvr.name);

        		}
        	});
        	
        }
        System.out.println("ColumnMetadata records = " + columnMetaRecCount);
        System.out.println("ColumnMetadata records = " + columnMetaDictionary.size());
        // dictionary size can be smaller as _studies_consents holds nested variables in it's concept path.
        System.out.println("Dictionary data table records = " + dictionary.size());
        System.out.println("Dictionary variable records = " + dictionaryTotalVars);
        
        System.out.println("Non-ingested or invalid metadata:");
        nonIngestedMetaRecords.forEach(str -> {
        	System.err.println(str);
        });
        System.out.println(dictKS.size());
        System.out.println(cmdKS.size());
    }

	private void stigmatizedVariables() {
		readStigmatizedVariables();
		columnMetaDictionary.forEach((phs,variables) -> {
			variables.variables.forEach((key,variable) -> {
				String HPDS_PATH = variable.getMetadata().containsKey("columnmeta_HPDS_PATH") ?
						variable.getMetadata().get("columnmeta_HPDS_PATH"): "";
				
				if(HPDS_PATH.isBlank()) {
					System.err.println("HPDS_PATH MISSING FOR - " + phs + ":" + key);
				} else {
					
					if(stigmatizedPaths.contains("HPDS_PATH")) {
						variable.getMetadata().put("columnmeta_is_stigmatized", "true");
					} else {
						variable.getMetadata().put("columnmeta_is_stigmatized", "false");

					}
					
				}
				
			});
		});
	}

	private void readStigmatizedVariables() {
		try(BufferedReader buffer = Files.newBufferedReader(Paths.get(inputDirectory + "conceptsToRemove.csv"))) {
			
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

	private void buildHarmonizedMetaDictionary() {
		for(File studyFolder : new File(inputDirectory).listFiles()) {
        	if(studyFolder.isFile()) continue;
        	if(studyFolder.getName().contains("hrmn")) {
				if(Arrays.stream(new File(studyFolder, "rawData")
		                .list((file, name)->{
		                    return name.endsWith(".json");}
		                )) == null) continue;
		                
		        	Arrays.stream(new File(studyFolder, "rawData")
		                .list((file, name)->{
		                    return name.endsWith(".json");}
		                )).forEach(file -> {
		                	harmonizedMetaDictionary.putAll(DefaultJsonDataDictionaryBuilder.buildHarmonized(new File(studyFolder.getAbsolutePath()+"/rawData/"+ file)));
		                });
				//File f = new File("./data/babyhug/rawData/babyhug_metadata.json");
        	}
		}			
	}

	private void buildJsonMetaDictionary() {
		for(File studyFolder : new File(inputDirectory).listFiles()) {
        	if(studyFolder.isFile()) continue;
        	if(studyFolder.getName().contains("hrmn")) continue;
			if(Arrays.stream(new File(studyFolder, "rawData")
	                .list((file, name)->{
	                    return name.endsWith("metadata.json");}
	                )) == null) continue;
	                
	        	Arrays.stream(new File(studyFolder, "rawData")
	                .list((file, name)->{
	                    return name.endsWith("metadata.json");}
	                )).forEach(file -> {
	                	jsonMetaDictionary.putAll(DefaultJsonDataDictionaryBuilder.build(new File(studyFolder.getAbsolutePath()+"/rawData/"+ file)));
	                });
			//File f = new File("./data/babyhug/rawData/babyhug_metadata.json");
		}	
		//return null; //DefaultJsonDataDictionaryBuilder.build(f);
		
	}

	private void buildVarTags() {
		for(Entry<String, TopmedDataTable> columnDict: columnMetaDictionary.entrySet()) {
			
			columnDict.getValue().variables.forEach((varid, var) -> {
				var.buildTags();
			});
			columnDict.getValue().generateTagMap();
		}
		
	}

	private void mergeDictionaries() {
		for(Entry<String, TopmedDataTable> columnDict: columnMetaDictionary.entrySet()) {
			if(fhsDictionary.containsKey(columnDict.getKey())) {
				TopmedDataTable fhsTDT = fhsDictionary.get(columnDict.getKey());
				// set dt metatags will ignore any fhs meta that matches column meta as column meta is truth of hpds data
				for(Entry<String,String> metadata: fhsTDT.metadata.entrySet()) {
					if(metadata.getValue().isEmpty()) continue;
					if(columnDict.getValue().metadata.containsKey(metadata.getKey())) {
						String newMetaKey = "columnmeta_" + metadata.getKey();
						String newMetaVal = columnDict.getValue().metadata.get(metadata.getKey());
						columnDict.getValue().metadata.put(newMetaKey,newMetaVal);
					}
					
					columnDict.getValue().metadata.put(metadata.getKey(), metadata.getValue());
				}
				for(Entry<String,TopmedVariable> fhsVariable: fhsTDT.variables.entrySet()) {
					if(columnDict.getValue().variables.containsKey(fhsVariable.getKey())) {
						for(Entry<String,String> fhsVarMeta :fhsVariable.getValue().getMetadata().entrySet()) {
							// overwrite all metadata derived from data dictionaries 
							// rename metadata for overwritten columnmeta meta.
							if(columnDict.getValue().variables.get(fhsVariable.getKey()).getMetadata().containsKey(fhsVarMeta.getKey())) {
								String cmvarnewkey = "columnmeta_" + fhsVarMeta.getKey();
								String cmvarnewval = columnDict.getValue().variables.get(fhsVariable.getKey()).getMetadata().get(fhsVarMeta.getKey());
								
								columnDict.getValue().variables.get(fhsVariable.getKey()).getMetadata().put(cmvarnewkey,cmvarnewval);
								columnDict.getValue().variables.get(fhsVariable.getKey()).getMetadata().put(fhsVarMeta.getKey(),fhsVarMeta.getValue());;
							}
							if(fhsVarMeta.getKey().equalsIgnoreCase("description")) {
								columnDict.getValue().variables.get(fhsVariable.getKey()).getMetadata().put("columnmeta_description", fhsVarMeta.getValue());
							}
						}
						
						
					}
				}
			}
			if(jsonMetaDictionary.containsKey(columnDict.getKey())) {
				TopmedDataTable jsonTDT = jsonMetaDictionary.get(columnDict.getKey());
				for(Entry<String,TopmedVariable> jsonVariable: jsonTDT.variables.entrySet()) {
					if(columnDict.getValue().variables.containsKey(jsonVariable.getKey())) {
						columnDict.getValue().variables.get(jsonVariable.getKey()).getMetadata().put("columnmeta_description",
								jsonVariable.getValue().getMetadata().get("columnmeta_description"));
					}
				}
				
			}
			if(columnDict.getKey().contains("DCC Harmonized data set")) {
				columnDict.getValue().variables.forEach((key,var ) -> {
					if(harmonizedMetaDictionary.containsKey(key)) {
						var.getMetadata().put("columnmeta_description", harmonizedMetaDictionary.get(key));
					} else if(key.startsWith("age_at_")) {
						String subKey = key.replace("age_at_", "");
						if(harmonizedMetaDictionary.containsKey(subKey)) {
							var.getMetadata().put("columnmeta_description", "Age at - " + harmonizedMetaDictionary.get(subKey));
						}
						
					} else if(key.startsWith("unit_")) {
						String subKey = key.replace("unit_", "");
						if(harmonizedMetaDictionary.containsKey(subKey)) {
							var.getMetadata().put("columnmeta_description", "Unit - " + harmonizedMetaDictionary.get(subKey));
						}
					} else if(key.startsWith("unit_")) {
						String subKey = key.replace("unit_", "");
						if(harmonizedMetaDictionary.containsKey(subKey)) {
							var.getMetadata().put("columnmeta_description", "Unit - " + harmonizedMetaDictionary.get(subKey));
						}
					}
					/// no metadata for the following harmonized variables 
					if(key.equals("ethnicity_1")) {
						var.getMetadata().put("columnmeta_description", "Ethnicity");
					} 
					if(key.equals("unit_ethnicity_1")) {
						var.getMetadata().put("columnmeta_description", "Ethnicity (Unit)");
					} 
					if(key.equals("race_1")) {
						var.getMetadata().put("columnmeta_description", "Race");
					} 
					if(key.equals("unit_race_1")) {
						var.getMetadata().put("columnmeta_description", "Race (Unit)");
					} 
				});
			} // check for non-decoded variable names
			
				
			
		}
		
	}

	private String buildVariableConceptPath(TopmedVariable variable) {
		return "\\" +
				variable.getStudyId().split("\\.")[0] + "\\"+
				variable.getDtId().split("\\.")[0]+ "\\"+
				variable.getVarId().split("\\.")[0]+"\\"+
		    	variable.getMetadata().get("name")+"\\";
	}

    private TreeMap<String, TopmedDataTable> readDictionary() {
        try(ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(JAVABIN)));){
            return (TreeMap<String, TopmedDataTable>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new TreeMap<>();
    }

    private void writeDictionary() {
        try(ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(JAVABIN)))){
            oos.writeObject(columnMetaDictionary);
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
			String jsonOut = mapper.writeValueAsString(columnMetaDictionary);
			
			Files.write(Paths.get(outputDirectory + "dictionary.json"), jsonOut.getBytes());
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    private TopmedDataTable loadDataTable(String pathname) throws IOException {
        File dataDictFile = new File(pathname);
		Document doc = Jsoup.parse(dataDictFile, "UTF-8");
        TopmedDataTable topmedDataTable = new TopmedDataTable(doc, dataDictFile.getName());
        return topmedDataTable;
    }

    public static void main(String[] args) throws IOException {

        new RawDataImporter(args[0]).run();
    }
}
