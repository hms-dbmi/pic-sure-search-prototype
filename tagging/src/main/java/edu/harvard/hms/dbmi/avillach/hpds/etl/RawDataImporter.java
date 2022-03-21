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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.math.NumberUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;

public class RawDataImporter {

    private static final String JAVABIN = "./data/dictionary.javabin";
    private TreeMap<String, TopmedDataTable> fhsDictionary;
    private String inputDirectory;

    private TreeMap<String, TopmedDataTable> columnMetaDictionary;

    private String dictionaryType = "xml";
    
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
        System.out.println(inputDirectory);
        for(File studyFolder : new File(inputDirectory).listFiles()) {
        	if(studyFolder.isFile()) continue;
            if(studyFolder!=null) {
            	// needs to be changed to be for any xml
            	Arrays.stream(new File(studyFolder, "rawData")
                    .list((file, name)->{
                        return name.endsWith("data_dict.xml");}
                    )).forEach((table)->{
                TopmedDataTable topmedDataTable;
                try {
                    topmedDataTable = loadDataTable(studyFolder.getAbsolutePath()+"/rawData/"+table);
                    fhsDictionary.put(topmedDataTable.metadata.get("id").replaceAll("\\.v.*", ""), topmedDataTable);
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
                    String tableId = split[2] + "." + split[3];
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
        
        
        System.out.println("###-Syncing values to column metadata-###");
        long columnMetaRecCount = 0;
        try(BufferedReader buffer = Files.newBufferedReader(Paths.get(inputDirectory + "/columnMeta.csv"))) {
        	
        	RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();
        	
        	CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(buffer)
        			.withCSVParser(rfc4180Parser);
        	
        	CSVReader csvreader = csvReaderBuilder.build();
        	
        	csvreader.forEach(columnMetaCSVRecord -> {
        		
        		ColumnMetaCSVRecord csvr = new ColumnMetaCSVRecord(columnMetaCSVRecord);
        		String[] concept = csvr.name.substring(1,csvr.name.length() - 1).split("\\\\");
        		//System.out.println(arr[3]);
        		String dt = null; // = csvr.name.split("\\\\").length > 0 ? csvr.name.split("\\\\")[2] : null;
        		int studyDepth = concept.length;
        		
        		if(studyDepth == 4) {
        			dt = concept[1];
        		}
        		if(studyDepth == 3) {
        			dt = concept[1];
        		}
        		if(studyDepth == 2) {
        			dt = concept[0];
        		}
        		if(studyDepth == 1) {
        			dt = concept[0];
        		}
        		if(dt != null) {
        			if(columnMetaDictionary.containsKey(dt)) {
        				TopmedVariable var =  new TopmedVariable(columnMetaDictionary.get(dt), csvr);
        				columnMetaDictionary.get(dt).variables.put(var.getVarId(), var);
        			} else {
        				columnMetaDictionary.put(dt, new TopmedDataTable(csvr));
        			}
        		}
        	});
        	
        	columnMetaRecCount = csvreader.getLinesRead();
        }
        
        
        for(TopmedDataTable table : fhsDictionary.values()) {
            Collection<TopmedVariable> variables = table.variables.values();
            table.generateTagMap();
            
            for(TopmedVariable variable : variables) {
            	variable.getMetadata().put("HPDS_PATH", buildVariableConceptPath(variable));
                tags.addAll(variable.getMetadata_tags());
                tags.addAll(variable.getValue_tags());
            }
        }
        // merge xml dictionaries to column metadata
        mergeDictionaries();
        
        buildVarTags();
        
        writeDictionary();
        
        TreeMap<String, TopmedDataTable> dictionary = readDictionary();
        
        Set<String> invalidDict = new HashSet<String>();
        AtomicInteger dictionaryTotalVars = new AtomicInteger();
        dictionary.keySet().forEach(key -> {
        	        
        	dictionary.get(key).variables.values().forEach((TopmedVariable value) -> {
        		dictionaryTotalVars.getAndIncrement();
        		if(value.getMetadata().containsKey("HPDS_PATH")) {
        			System.out.println(value.getMetadata().get("HPDS_PATH"));
        		} else {
        			System.err.println("Dictionary variable missing required metadata=" + value.getStudyId() + " - " + value.getVarId());
        		}
        	});
        	/* this method call is no longer valid as non dbgap studies are not 4 level of concept depth
        	 * each dictionary variable has an hpds_path saved in it's metadata will display that instead
        	 * 
            dictionary.get(key).variables.values().forEach((TopmedVariable value) -> { 
	        	System.out.println(buildVariableConceptPath(value));
            });*/
            
        });        
        System.out.println("ColumnMetadata records = " + columnMetaRecCount);
        // dictionary size can be smaller as _studies_consents holds nested variables in it's concept path.
        System.out.println("Dictionary data table records = " + dictionary.size());
        System.out.println("Dictionary variable records = " + dictionaryTotalVars);
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
					String metakey = "dict_" + metadata.getKey();
					columnDict.getValue().metadata.put(metakey, metadata.getValue());
				}
				for(Entry<String,TopmedVariable> fhsVariable: fhsTDT.variables.entrySet()) {
					if(columnDict.getValue().variables.containsKey(fhsVariable.getKey())) {
						for(Entry<String,String> fhsVarMeta :fhsVariable.getValue().getMetadata().entrySet()) {
							if(!columnDict.getValue().variables.get(fhsVariable.getKey()).getMetadata().containsKey(fhsVarMeta.getKey())) {
								columnDict.getValue().variables.get(fhsVariable.getKey()).getMetadata().put(fhsVarMeta.getKey(),fhsVarMeta.getValue());;
							}
						}
					}
				}
			}
			
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
    }

    private TopmedDataTable loadDataTable(String pathname) throws IOException {
        File dataDictFile = new File(pathname);
		Document doc = Jsoup.parse(dataDictFile, "UTF-8");
        TopmedDataTable topmedDataTable = new TopmedDataTable(doc, dataDictFile.getName());
        return topmedDataTable;
    }

    public static void main(String[] args) throws IOException {
    	args = new String[] {"./data/"};
        new RawDataImporter(args[0]).run();
    }
}
