package edu.harvard.hms.dbmi.avillach.hpds.etl;

import edu.harvard.hms.dbmi.avillach.hpds.HPDSPathLookup;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RawDataImporter {

    private static final String JAVABIN = "/usr/local/docker-config/search/dictionary.javabin";
    private TreeMap<String, TopmedDataTable> fhsDictionary;
    private String inputDirectory;

    public RawDataImporter(String inputDirectory) {
		this.inputDirectory = inputDirectory;
	}

	public void run() throws IOException {
        fhsDictionary = new TreeMap<>();

        //		if(! new File(TMP_DICTIONARY_JAVABIN).exists()) {

        for(File studyFolder : new File(inputDirectory).listFiles()) {
            if(studyFolder!=null) {
            	Arrays.stream(new File(studyFolder, "rawData")
                    .list((file, name)->{
                        return name.endsWith("data_dict.xml");}
                    )).forEach((table)->{
                TopmedDataTable topmedDataTable;
                try {
                    topmedDataTable = loadDataTable(studyFolder.getAbsolutePath()+"/rawData/"+table);
                    fhsDictionary.put(topmedDataTable.metadata.get("id"), topmedDataTable);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                });
            }
        }

        for(File studyFolder : new File(inputDirectory).listFiles()) {
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
        for(TopmedDataTable table : fhsDictionary.values()) {
            Collection<TopmedVariable> variables = table.variables.values();
            table.generateTagMap();
            
            for(TopmedVariable variable : variables) {
            	variable.getMetadata().put("HPDS_PATH", "\\" +
            			variable.getStudyId().split("\\.")[0] + "\\"+
            			variable.getDtId().split("\\.")[0]+ "\\"+
            			variable.getVarId().split("\\.")[0]+"\\");
                tags.addAll(variable.getMetadata_tags());
                tags.addAll(variable.getValue_tags());
            }
        }

        writeDictionary();

        TreeMap<String, TopmedDataTable> dictionary = readDictionary();
        dictionary.keySet().forEach(key -> {
            dictionary.get(key).variables.values().forEach((TopmedVariable value) -> { 
	        	System.out.println("\\" + value.getStudyId().split("\\.")[0] + "\\" + value.getDtId().split("\\.")[0] + "\\" + value.getVarId().split("\\.")[0] + "\\");
            });
        });

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
            oos.writeObject(fhsDictionary);
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
        new RawDataImporter(args[0]).run();
    }
}
