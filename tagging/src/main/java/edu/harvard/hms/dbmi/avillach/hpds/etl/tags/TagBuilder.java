package edu.harvard.hms.dbmi.avillach.hpds.etl.tags;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import edu.harvard.hms.dbmi.avillach.hpds.etl.dict.factory.DictionaryFactory;

/**
 * 
 * This class should handle all of the tag building and tag filtering
 * that is currently being done in the Entity classes
 * 
 * @author TDeSain
 *
 */
public class TagBuilder {

	private static Set<String> STOP_WORDS = populateStopWordListFromDataFile();

	/**
	 * Method to populate the stopwords set
	 */
	public static HashSet<String> populateStopWordListFromDataFile() {
		try {
				
			return Sets.newHashSet(Files.readLines(Paths.get(DictionaryFactory.STOP_WORDS_FILE).toFile(),StandardCharsets.UTF_8));
			
		} catch (Exception e) {			
			e.printStackTrace();
			System.err.println(e);
		}
		return null;
	
	}
	/**
	 * List of element keys to ignore
	 */
	private static List<String> IGNORE_META_KEYS = List.of(
			"hashed_var_id"
		);
	/**
	 * 
	 * This method will build the metadata tags collections for new search.
	 * 
	 */
	public void buildTags(TreeMap<String, TopmedDataTable> hpdsDictionary) {
		for(Entry<String, TopmedDataTable> hpdsDictEntry: hpdsDictionary.entrySet()) {
			
			hpdsDictEntry.getValue().variables.forEach((varid, var) -> {
				var.getMetadata().forEach((k,v) -> {
					try {
						if(IGNORE_META_KEYS.contains(k)) return;
						//TopmedVariable tvMethods = TopmedVariable.class.getDeclaredConstructor().newInstance();
						
						// phs to upper and lower
						if(!var.getStudyId().isBlank()) {
							// cannot filter lowercase objects as filter tags always returns an uppercases value
							// which is fine as we do not want to filter any study ids.
							var.getMetadata_tags().add(var.getStudyId().toLowerCase());
							var.getMetadata_tags().addAll(this.filterTags(var.getStudyId().toUpperCase()));
						}
						
						// pht to upper and lower
						if(!var.getDtId().isBlank()) {
							var.getMetadata_tags().add(var.getDtId().toLowerCase());
							var.getMetadata_tags().addAll(this.filterTags(var.getDtId().toUpperCase()));
						}
						
						
						// phv to upper and lower
						var.getMetadata_tags().add(var.getVarId().toLowerCase());
						var.getMetadata_tags().addAll(this.filterTags(var.getVarId().toUpperCase()));
												
						// data type
						var.getMetadata_tags().addAll(this.filterTags(var.getMetadata().get("columnmeta_data_type").toUpperCase()));
						
						// variable encoded name
						var.getMetadata_tags().addAll(this.filterTags(var.getMetadata().get("columnmeta_name").toUpperCase()));
						
						var.getMetadata_tags().addAll(this.filterTags(var.getMetadata().get("derived_var_description").toUpperCase()));
						
						var.getMetadata_tags().addAll(this.filterTags(var.getMetadata().get("derived_study_abv_name").toUpperCase()));
						
						var.getMetadata_tags().addAll(this.filterTags(var.getMetadata().get("derived_study_description").toUpperCase()));
						
						
					} catch (IllegalArgumentException | SecurityException e) {
						// TODO exception handling
						e.printStackTrace();
					}
				});
				// add values to metadata tags and value tags
				for(String value: var.getValues()) {
					try {
						var.getValue_tags().addAll(this.filterTags(value.toUpperCase()));
						var.getMetadata_tags().addAll(this.filterTags(value.toUpperCase()));
					} catch (IllegalArgumentException |
						 SecurityException e) {
						e.printStackTrace();
					}
		
				}
				for(String valuesTolower: var.getValue_tags()) {
					var.allTagsLowercase.add(valuesTolower.toLowerCase());
				}
				for(String metatagsTolower: var.getMetadata_tags()) {
					var.allTagsLowercase.add(metatagsTolower.toLowerCase());
				}
			});
			this.generateTagMap(hpdsDictEntry.getValue());
		}		
	}
	
	/**
	 * 
	 * Used to filter out tags
	 * value will be split by punctuation then each value in the resulting array will be filtered based on the criteria in this method
	 * 
	 * @param value
	 * @return
	 */
	public List<String> filterTags(String value) {
		return Arrays.asList(value.split("[\\s\\p{Punct}]"))
				.stream().filter((val2)->{
					return val2.length() > 1 
							&& !val2.isBlank()
							&& !val2.matches("^\\d+$")
							&& !STOP_WORDS.stream().anyMatch(val2::equalsIgnoreCase)
							&& !val2.toUpperCase().matches("^V\\d+$");}).map((String var)->{
								return var.toUpperCase();}).collect(Collectors.toList());
	}
	
	/**
	 * Generates the tagMap
	 * 
	 * @param dt
	 */
	public void generateTagMap(TopmedDataTable dt) {
		dt.tagMap = new HashMap<>();
		Set<String> tags = new HashSet<String>();
		for(TopmedVariable variable : dt.variables.values()) {
			tags.addAll(variable.getMetadata_tags());
			tags.addAll(variable.getValue_tags());
		}
		for(String tag : tags) {
			dt.tagMap.put(tag, new HashSet<TopmedVariable>());
		}
		for(TopmedVariable variable : dt.variables.values()) {
			for(String tag : variable.getMetadata_tags()) {
				dt.tagMap.get(tag).add(variable);
			}
			for(String tag : variable.getValue_tags()) {
				dt.tagMap.get(tag).add(variable);
			}
		}
	}
	
}
