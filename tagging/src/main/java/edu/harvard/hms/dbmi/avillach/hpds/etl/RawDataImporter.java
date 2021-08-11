package edu.harvard.hms.dbmi.avillach.hpds.etl;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RawDataImporter {

    private static final String TMP_DICTIONARY_JAVABIN = "/tmp/dictionary.javabin";
    private TreeMap<String, TopmedDataTable> fhsDictionary;
    private String inputDirectory;

    public RawDataImporter(String inputDirectory) {
		this.inputDirectory = inputDirectory;
	}

	public void run() {
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
            for(TopmedVariable variable : variables) {
                tags.addAll(variable.getMetadata_tags());
                tags.addAll(variable.getValue_tags());
            }
        }

        writeDictionary();

        TreeMap<String, TopmedDataTable> dictionary = readDictionary();
        dictionary.keySet().forEach(key -> {
            System.out.println("variable : " + key);
            dictionary.get(key).variables.values().forEach(value -> { 
	        	value.getValue_tags().forEach(System.out::println);
	        	value.getMetadata_tags().forEach(System.out::println);
            });
        });

    }

    private TreeMap<String, TopmedDataTable> readDictionary() {
        try(ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(TMP_DICTIONARY_JAVABIN)));){
            return (TreeMap<String, TopmedDataTable>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new TreeMap<>();
    }

    private void writeDictionary() {
        try(ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(TMP_DICTIONARY_JAVABIN)))){
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

    public static void main(String[] args) {
        new RawDataImporter(args[0]).run();
    }
}
