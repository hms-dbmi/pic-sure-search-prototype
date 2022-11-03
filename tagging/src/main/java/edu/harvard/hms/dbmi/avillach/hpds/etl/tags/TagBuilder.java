package edu.harvard.hms.dbmi.avillach.hpds.etl.tags;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.cxf.helpers.FileUtils;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;

/**
 * 
 * This class should handle all of the tag building and tag filtering
 * that is currently being done in the Entity classes
 * 
 * @author TDeSain
 *
 */
public class TagBuilder {

	private static final List<String> EXCLUDED_WORDS_LIST = List.of(
			"a",
			"about",
			"again",
			"AH",
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
			"as",
			"associated",
			"associated",
			"at",
			"be",
			"because",
			"been",
			"before",
			"being",
			"between",
			"both",
			"broad",
			"but",
			"by",
			"calculated",
			"can",
			"cardia",
			"could",
			"data",
			"decimal",
			"derived",
			"desaturation",
			"descriptions",
			"dictionaries",
			"did",
			"do",
			"documentation",
			"documents",
			"does",
			"done",
			"dopm",
			"due",
			"during",
			"each ",
			"ehough",
			"either",
			"encoded",
			"end",
			"enum",
			"especially",
			"etc",
			"extracted",
			"fhs",
			"find",
			"format",
			"format",
			"found",
			"framingham",
			"from",
			"further",
			"getting",
			"had",
			"happen",
			"has",
			"have",
			"having ",
			"here",
			"how",
			"however",
			"i",
			"if",
			"in",
			"inserted",
			"institute",
			"integer",
			"into",
			"is",
			"it",
			"its",
			"itself",
			"just",
			"kg",
			"km",
			"left",
			"made",
			"main",
			"mainly",
			"make",
			"may",
			"me",
			"mg",
			"might",
			"might",
			"ml",
			"mm",
			"most",
			"mostly",
			"must",
			"nearly",
			"neither",
			"nhlbi",
			"no",
			"nor",
			"not",
			"now",
			"numeric",
			"NWD100097",
			"obtained",
			"of",
			"often",
			"often",
			"often",
			"on",
			"one",
			"or",
			"our",
			"overall",
			"participant",
			"people",
			"perhaps",
			"please",
			"pmid",
			"position",
			"possible",
			"probably",
			"quite",
			"rather",
			"really",
			"reason",
			"regarding",
			"repository",
			"right",
			"sas",
			"sb",
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
			"slice",
			"so",
			"some",
			"study",
			"submitted",
			"such",
			"sure",
			"system",
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
			"time",
			"to",
			"too",
			"uab",
			"up",
			"upon",
			"values",
			"various",
			"very",
			"was",
			"we",
			"were",
			"what",
			"when",
			"whi",
			"which",
			"while",
			"with",
			"within",
			"without",
			"would",
			"XX",
			"XXXXX",
			"XXXXXXXXXXXXXXXXXXXX",
			"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
			"yes"
			);
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
						var.getValue_tags().addAll(TopmedVariable.class.getDeclaredConstructor().newInstance().filterTags(value.toUpperCase()));
						var.getMetadata_tags().addAll(TopmedVariable.class.getDeclaredConstructor().newInstance().filterTags(value.toUpperCase()));
					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException | NoSuchMethodException | SecurityException e) {
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
			hpdsDictEntry.getValue().generateTagMap();
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
							&& !EXCLUDED_WORDS_LIST.stream().anyMatch(val2::equalsIgnoreCase)
							//&& !EXCLUDED_WORDS_LIST.contains(val2.toUpperCase()) 
							&& !val2.toUpperCase().matches("^V\\d+$");}).map((String var)->{
								return var.toUpperCase();}).collect(Collectors.toList());
	}
	
	/**
	 * Generates the tagMap Hash Map which is basically the index used by search
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
	/**
	 * This method can be called to load the Excluded word list from 
	 * a separate data file.
	 * 
	 * Not yet implemented see comments below
	 */
	public void populateExcludedWordListFromDataFile(File excludedWordListFile) {
		try {
			// Excluded word list cannot be final and probably should be a Set instead of a list
			// line below can be uncommented to populate the list from a file with those changes
			//EXCLUDED_WORDS_LIST = new HashSet<String>(FileUtils.readLines(excludedWordListFile));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}
