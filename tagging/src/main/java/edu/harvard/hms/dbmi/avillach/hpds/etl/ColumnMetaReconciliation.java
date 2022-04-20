package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.*;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;

public class ColumnMetaReconciliation {
    private static final String JAVABIN = "/usr/local/docker-config/search/dictionary.javabin";
	private static final String JAVABIN2 = "/usr/local/docker-config/search/dictionary_original.javabin";

    private static String[] stopWords = new String[] {
    		"associated",
    		"as",
    		"",
    		"get",
    		"broad",
    		"calculated",
    		"cardia",
    		"data",
    		"decimal",
    		"derived",
    		"desaturation",
    		"descriptions",
    		"dictionaries",
    		"documentation",
    		"documents",
    		"dopm",
    		"encoded",
    		"end",
    		"enum",
    		"extracted",
    		"find",
    		"format",
    		"getting",
    		"happen",
    		"inserted",
    		"institute",
    		"integer",
    		"left",
    		"main",
    		"me",
    		"might",
    		"nhlbi",
    		"now",
    		"numeric",
    		"often",
    		"one",
    		"participant",
    		"people",
    		"please",
    		"position",
    		"possible",
    		"probably",
    		"reason",
    		"repository",
    		"right",
    		"sas",
    		"sb",
    		"slice",
    		"submitted",
    		"sure",
    		"system",
    		"time",
    		"too",
    		"uab",
    		"up",
    		"values",
    		"yes",
    		"a",
    		"about",
    		"again",
    		"all",
    		"almost",
    		"also",
    		"although",
    		"always",
    		"among",
    		"an",
    		"and",
    		"another",
    		"any",
    		"are",
    		"associated",
    		"at",
    		"be",
    		"because",
    		"been",
    		"before",
    		"being",
    		"between",
    		"both",
    		"but",
    		"by",
    		"can",
    		"could",
    		"did",
    		"do",
    		"does",
    		"done",
    		"due",
    		"during",
    		"each ",
    		"either",
    		"ehough",
    		"especially",
    		"etc",
    		"format",
    		"my",
    		"he",
    		"name",
    		"her",
    		"found",
    		"from",
    		"further",
    		"had",
    		"has",
    		"have",
    		"having ",
    		"here",
    		"how",
    		"however",
    		"i",
    		"if",
    		"in",
    		"into",
    		"is",
    		"it",
    		"its",
    		"itself",
    		"just",
    		"kg",
    		"km",
    		"made",
    		"mainly",
    		"make",
    		"may",
    		"mg",
    		"might",
    		"ml",
    		"mm",
    		"most",
    		"mostly",
    		"must",
    		"nearly",
    		"neither",
    		"no",
    		"nor",
    		"obtained",
    		"often",
    		"often",
    		"on",
    		"our",
    		"overall",
    		"perhaps",
    		"pmid",
    		"quite",
    		"rather",
    		"really",
    		"regarding",
    		"seem",
    		"seen",
    		"several",
    		"should",
    		"show",
    		"showed",
    		"shown",
    		"shows",
    		"significantly",
    		"since",
    		"so",
    		"some",
    		"such",
    		"than",
    		"that",
    		"the",
    		"their",
    		"them",
    		"then",
    		"there",
    		"therefore",
    		"these",
    		"they",
    		"this",
    		"those",
    		"through",
    		"thus",
    		"to",
    		"upon",
    		"various",
    		"very",
    		"was",
    		"we",
    		"were",
    		"what",
    		"when",
    		"which",
    		"while",
    		"with",
    		"within",
    		"without",
    		"would",
    		"phs000667","phs000420","phs000400","phs000377","phs000342","phs000301","phs000283","phs000226"
    		};
    
    private static List<String> stigmatized = new ArrayList<String>();
    
	public static void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("/usr/local/docker-config/search/conceptsToRemove.txt"));
		String line = reader.readLine();
		while(line!=null) {
			stigmatized.add(line);
			line = reader.readLine();
		}
		
		TreeMap<String, ColumnMeta>[] metaStore = new TreeMap[1];
		try (ObjectInputStream objectInputStream = new ObjectInputStream(new GZIPInputStream(new FileInputStream("/opt/local/hpds/columnMeta.javabin")));){
			TreeMap<String, ColumnMeta> _metastore = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();
			TreeMap<String, ColumnMeta> metastoreScrubbed = new TreeMap<String, ColumnMeta>();
			for(Entry<String,ColumnMeta> entry : _metastore.entrySet()) {
				metastoreScrubbed.put(entry.getKey().replaceAll("\\ufffd",""), entry.getValue());
			}
			metaStore[0] = metastoreScrubbed;
			objectInputStream.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		} 
		TreeMap<String, TopmedDataTable> dictionary = readDictionary();
		
		TreeMap<String, TopmedDataTable> original_dictionary = readDictionary2();
		
		TreeMap<String, TopmedDataTable> updatedDictionary = new TreeMap<>();
		
		for(Entry<String, TopmedDataTable> entry : dictionary.entrySet()) {
			TreeMap<String, String> metadata = entry.getValue().metadata;
			String newKey;
			if(!metadata.get("columnmeta_study_id").equals(metadata.get("columnmeta_id"))) {
				newKey = metadata.get("columnmeta_study_id") + "_" + metadata.get("columnmeta_id");
				updatedDictionary.put(newKey, entry.getValue());				
			}else {
				newKey = entry.getKey();
				updatedDictionary.put(entry.getKey(), entry.getValue());
			}
			System.out.println(newKey);
		}
		
		List<String> stopWordList = Arrays.asList(stopWords).stream().map((word)->{return word.toLowerCase();}).collect(Collectors.toList());
		for(Entry<String, TopmedDataTable> entry : dictionary.entrySet()) {
			System.out.println("Cleaning : " + entry.getKey());
			entry.getValue().variables.entrySet().forEach((variable)->{
				variable.getValue().getMetadata_tags().removeIf((string)->{
					return stopWordList.contains(string.toLowerCase());
				});
				variable.getValue().getValue_tags().removeIf((string)->{
					return stopWordList.contains(string.toLowerCase());
				});
				variable.getValue().allTagsLowercase.removeIf((string)->{
					return stopWordList.contains(string.toLowerCase());
				});
			});
		}
		
		List<String> tagsToRemove = List.of("columnmeta_patient_count","columnmeta_max","columnmeta_min","columnmeta_name","columnmeta_HPDS_PATH","columnmeta_var_id","columnmeta_var_group_description","columnmeta_description","columnmeta_data_type","columnmeta_study_id","columnmeta_var_group_id","columnmeta_observation_count");
		for(Entry<String, TopmedDataTable> entry : dictionary.entrySet()) {
			System.out.println("Cleaning : " + entry.getKey());
			entry.getValue().variables.entrySet().forEach((variable)->{
				variable.getValue().getMetadata_tags().removeAll(tagsToRemove);
				variable.getValue().allTagsLowercase.removeAll(tagsToRemove);
			});
		}
		
		for(Entry<String, TopmedDataTable> entry : dictionary.entrySet()) {
			System.out.println("Augmenting : " + entry.getKey());
			entry.getValue().variables.entrySet().forEach((variable)->{
				TopmedDataTable dt = getDtFromOrig(original_dictionary, variable);
				TopmedVariable var = getVariableFromOrig(dt, variable);
				variable.getValue().getMetadata().put("columnmeta_var_id", variable.getValue().getVarId());
				if(var!=null) {
					variable.getValue().setDtId(var.getDtId().split("\\.")[0]);
					variable.getValue().setStudyId(var.getStudyId().split("\\.")[0]);
					if(dt.metadata.get("description")!=null) {
						variable.getValue().getMetadata().put("columnmeta_var_group_description", dt.metadata.get("description"));	
					}else if(var.getMetadata().get("dataTableDescription")!=null) {
						variable.getValue().getMetadata().put("columnmeta_var_group_description", var.getMetadata().get("dataTableDescription"));	
					}else {
						variable.getValue().getMetadata().put("columnmeta_var_group_description", "");	
					}
				} else {
					if(variable.getValue().getMetadata().get("columnmeta_description").equals(variable.getValue().getMetadata().get("columnmeta_name"))) {
						variable.getValue().getMetadata().put("columnmeta_description","");
					}
					if(variable.getValue().getStudyId().startsWith("phs")) {
						System.out.println("Not fixing up " + variable.getValue().getVarId());
					}
				}
				if(stigmatized.contains(variable.getValue().getMetadata().get("columnmeta_HPDS_PATH"))){
					variable.getValue().getMetadata().put("columnmeta_is_stigmatized", "true");
					variable.getValue().getMetadata_tags().add(variable.getValue().getMetadata().get("columnmeta_HPDS_PATH"));
				}
			});
		}
		
		Set<String> variablesInHPDS = metaStore[0].keySet();
		TreeSet<String> variablesInDictionary = new TreeSet<String>();
		List<String> categoriesMissingFromHPDS = new ArrayList<String>();
		
		dictionary.entrySet().parallelStream().forEach((entry)->{
			System.out.println("Updating Values : " + entry.getKey());
			List<String> variablesNotInHPDS = new ArrayList<String>();
			entry.getValue().variables.entrySet().forEach((variable)->{
				variablesInDictionary.add(variable.getValue().getMetadata().get("columnmeta_HPDS_PATH"));
				HashMap<String, String> updatedValues = new HashMap<>();
				ColumnMeta variableMeta = metaStore[0].get(variable.getValue().getMetadata().get("columnmeta_HPDS_PATH"));
				if(variableMeta!=null) {
					if(variableMeta.isCategorical()) {
						variable.getValue().getMetadata().put("columnmeta_data_type","categorical");
						variable.getValue().setIs_categorical(true);
						variable.getValue().setIs_continuous(false);
						Set<String> categories = new TreeSet(variable.getValue().getValues().values());
						Set<String> metaCategories = new TreeSet(variableMeta.getCategoryValues());
						Set<String> categoriesMissingFromDictionary = categoriesMissingFromDictionary(categories, metaCategories);
						if(categoriesMissingFromDictionary.size()>1) {
							categoriesMissingFromHPDS.add(variable.getKey() + " is missing these categories in the dictionary but they exist in HPDS : " + categoriesMissingFromDictionary + "\n");
						}
						variable.getValue().getValues().entrySet().forEach((value)->{
							if(variableMeta!=null) {
								if(variableMeta.getCategoryValues().contains(value.getValue())){
									updatedValues.put(value.getKey(), value.getValue());
								}
							}
						});
						if(updatedValues.size()>0) {
							variable.getValue().setValues(updatedValues);
							System.out.println("Updated Values For " + variable.getValue().getMetadata().get("columnmeta_HPDS_PATH"));
						}
					}else {
						variable.getValue().getMetadata().put("columnmeta_data_type","continuous");
						variable.getValue().setIs_categorical(false);
						variable.getValue().setIs_continuous(true);
						variable.getValue().getMetadata().put("columnmeta_max",variableMeta.getMax().toString());
						variable.getValue().getMetadata().put("columnmeta_min",variableMeta.getMin().toString());
						variable.getValue().setValues(new HashMap<String, String>());
					}
					variable.getValue().getValue_tags().removeIf((string)->{
						return stopWordList.contains(string.toLowerCase());
					});
					
				}else {
					System.out.println("Removing " + variable.getValue().getMetadata().get("columnmeta_HPDS_PATH"));
					variablesNotInHPDS.add(variable.getKey());
				}
			});
			variablesNotInHPDS.forEach((variable)->{
				entry.getValue().variables.remove(variable);
			});
		});
		
		SetView<String> missingFromDictionary = Sets.difference(variablesInHPDS, variablesInDictionary);
		System.out.println("In HPDS but not dictionary: " + missingFromDictionary.size());
		System.out.println(missingFromDictionary);
		
		SetView<String> missingFromHPDS = Sets.difference(variablesInDictionary, variablesInHPDS);
		System.out.println("In dictionary but not HPDS : " + missingFromHPDS.size());
		System.out.println(missingFromHPDS);
		
		
		System.out.println("Missing Categories From Dictionary : \n\n" + categoriesMissingFromHPDS);
		
		writeDictionary(updatedDictionary);
		
	}
	private static Set<String> categoriesMissingFromDictionary(Set<String> categories, Set<String> metaCategories) {
		return Sets.difference(metaCategories, categories);
	}

    private static TopmedVariable getVariableFromOrig(TopmedDataTable oldDt, Entry<String, TopmedVariable> variable) {
    	if(oldDt==null) {
    		return null;
    	}
    	TopmedVariable ret = oldDt
    			.variables
    			.get(variable
    					.getKey());
    	if(ret==null) {
    		return null;
    	}
    	return ret;
	}

    private static TopmedDataTable getDtFromOrig(TreeMap<String, TopmedDataTable> original_dictionary, Entry<String, TopmedVariable> variable) {
    	TopmedDataTable dt = null;
		List<Entry<String, TopmedDataTable>> entries = new ArrayList(original_dictionary.entrySet());
		for(int x = 0;x<entries.size()&&dt==null;x++) {
			if(entries.get(x).getKey().startsWith(variable.getValue().getDtId())){
				dt = entries.get(x).getValue();
			}
		}
		if(dt==null) {
			return null;
		}
		return dt;
	}

	private static TreeMap<String, TopmedDataTable> readDictionary() {
        try(ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(JAVABIN)));){
            return (TreeMap<String, TopmedDataTable>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new TreeMap<>();
    }
    
	private static TreeMap<String, TopmedDataTable> readDictionary2() {
		try(ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(JAVABIN2)));){
			return (TreeMap<String, TopmedDataTable>) ois.readObject();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		return new TreeMap<>();
	}

    private static void writeDictionary(TreeMap<String, TopmedDataTable> dictionary) {
        try(ObjectOutputStream ois = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(JAVABIN+"_updated")));){
            ois.writeObject(dictionary);
            ois.flush();
            ois.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
}
