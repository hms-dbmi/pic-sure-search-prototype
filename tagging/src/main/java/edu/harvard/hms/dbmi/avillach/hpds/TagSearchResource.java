package edu.harvard.hms.dbmi.avillach.hpds;

import edu.harvard.hms.dbmi.avillach.hpds.model.*;
import edu.harvard.hms.dbmi.avillach.hpds.model.domain.*;
import lombok.SneakyThrows;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Value;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

@Path("/pic-sure")
@Produces({"application/json"})
@Consumes({"application/json"})
public class TagSearchResource implements IResourceRS {

    public static final Comparator<SearchResult> SEARCH_RESULT_COMPARATOR = Comparator.comparing(SearchResult::getScore).reversed().thenComparing(result -> result.getResult().getMetadata().get("columnmeta_description"));
    private SortedMap<String, TopmedDataTable> fhsDictionary;
    private static final String JAVABIN = "/usr/local/docker-config/search/dictionary.javabin";
    private static final int INITIAL_RESULTS_SIZE = 20;
    private Map _projectMap;

    @Value("file:/usr/local/docker-config/search/fence_mapping.json")
    Resource fenceMapping;

    public TagSearchResource() {
        fhsDictionary = readDictionary();
    }

    public TagSearchResource(SortedMap<String, TopmedDataTable> fhsDictionary) {
        this.fhsDictionary = fhsDictionary;
    }

    @Override
    public ResourceInfo info(QueryRequest queryRequest) {
    	//need to return a value here so that PIC-SURE will know this resource is active
	    ResourceInfo info = new ResourceInfo();
        info.setName("Pic-Sure Dictionary Resource");
        return info;
    }

    @SneakyThrows
    @Override
    @POST
    @Path("/search")
    public SearchResults search(SearchRequest searchRequest) {
        final SearchQuery searchQuery = searchRequest.getQuery();
        final int resultOffset = searchQuery.getOffset();
        final int resultLimit = searchQuery.getLimit() > 0 ? searchQuery.getLimit() : INITIAL_RESULTS_SIZE;
        Map<Double, List<TopmedVariable>> results = new TreeMap<>();
        TreeMap<String,Integer> tagStats = new TreeMap<String, Integer>();
        Collection<TopmedDataTable> allTables = fhsDictionary.values();
    	for(TopmedDataTable table : allTables) {
            Map<Double, List<TopmedVariable>> search = table.searchVariables(searchQuery);
            if (search.size() == 0) {
                continue;
            }

            for(Double score : search.keySet()) {
                List<TopmedVariable> resultsForScore = results.get(score);
                if(resultsForScore==null) {
                    resultsForScore = new ArrayList<>();
                    results.put(score, resultsForScore);
                }
                resultsForScore.addAll(search.get(score));
             }
        }
        int numberOfVariables = 0;
        for(List<TopmedVariable> resultsForScore : results.values()) {
            numberOfVariables += resultsForScore.size();
        }
        final int numVars = numberOfVariables;

        Stream<TagResult> tagResults = Stream.empty();
        if (searchQuery.isReturnTags()) {
        	//flatten the tags from each score and each variable into a single list
        	Set<String> tags = results.values().parallelStream().flatMap(List::stream).map((TopmedVariable variable)->{
        		return Stream.concat(variable.getMetadata_tags().stream(),variable.getValue_tags().stream()).collect(Collectors.toSet());
        	}).flatMap(Set::stream).collect(Collectors.toSet());

            for(String tag : tags) {
                tagStats.put(tag, 0);
            }
            
            for(List<TopmedVariable> scoreResults : results.values()) {
                for(TopmedVariable result : scoreResults) {

                    for(String tag : result.getMetadata_tags()) {
                        tagStats.put(tag,tagStats.get(tag)+1);
                    }
                    for(String tag : result.getValue_tags()) {
                        tagStats.put(tag,tagStats.get(tag)+1);
                    }
                }
            }
            tagResults = tagStats.entrySet().stream()
                    .map(entry -> {
                    	return new TagResult(entry.getKey(), entry.getValue());
                    }).sorted(Comparator.comparing(TagResult::getScore).reversed());
            if(tagStats.size()>10) {
                tagResults = tagResults.filter(result -> 
                result.getScore() > 0 && (
                result.getTag().toLowerCase().matches("dcc harmonized data set") ||
        		result.getTag().matches("PHS\\d{6}+.*") || 
        		result.getTag().toUpperCase().matches("PHT\\d{6}+.*") ||
                getValueThatContainsKey(result.getTag().toUpperCase()) != null ||
                	(result.getScore() > numVars * .05 && result.getScore() < numVars * .95)
                ));
            }
        }

        // flatten the results for each score into a list of search results
        List<SearchResult> searchResults = results.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(topmedVariable -> {
                            if (searchQuery.getVariableValuesLimit() != null && searchQuery.getVariableValuesLimit() < topmedVariable.getValues().size()) {
                                return topmedVariable.copy(searchQuery.getVariableValuesLimit());
                            }
                            return topmedVariable;
                        })
                        .map(topmedVariable -> new SearchResult(topmedVariable, entry.getKey())))
                .sorted(SEARCH_RESULT_COMPARATOR)
                .collect(Collectors.toList());

        int searchResultsSize = searchResults.size();
//        if (!searchRequest.getQuery().isReturnAllResults()) {
//            searchResults = searchResults.subList(0, Math.min(INITIAL_RESULTS_SIZE, searchResults.size()));
//        }
        TagSearchResponse tagSearchResponse = new TagSearchResponse(
                tagResults.collect(Collectors.toList()),
                searchResults.subList(resultOffset, Math.min(searchResults.size(),resultOffset + resultLimit)),
                searchResultsSize
        );
        return new SearchResults()
                .setResults(tagSearchResponse);
    }

    @Override
    public QueryStatus query(QueryRequest queryJson) {
        return null;
    }

    @Override
    public QueryStatus queryStatus(String queryId, QueryRequest statusRequest) {
        return null;
    }

    @Override
    public Response queryResult(String queryId, QueryRequest resultRequest) {
        return null;
    }

    @Override
    @SneakyThrows
    @POST
    @Path("/query/sync")
    public Response querySync(QueryRequest queryRequest) {
        switch (queryRequest.getQuery().getEntityType()) {
	        case DATA_TABLE:
	            return getDataTable(queryRequest.getQuery().getId());
            default:
                throw new RuntimeException("Invalid Entity type");
        }
    }

	private Response getDataTable(String dataTableId) {
        TopmedDataTable topmedDataTable = fhsDictionary.get(String.valueOf(dataTableId));
        return Response.ok(topmedDataTable).build();
    }

    @Override
    @POST
    @Path("/query/format")
    public Response queryFormat(QueryRequest resultRequest) {
        return Response.ok(resultRequest.getQuery()).build();
    }


    private SortedMap<String, TopmedDataTable> readDictionary() {
        try(ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(JAVABIN)));){
            SortedMap<String, TopmedDataTable> dictionary = (SortedMap<String, TopmedDataTable>) ois.readObject();
            // Remove "values" key from TopmedVariable.medatada map -- they are redundant, they are also set in TopmedVariable.values
            dictionary.values().forEach(topmedDataTable -> {
                topmedDataTable.variables.values().forEach(topmedVariable -> topmedVariable.getMetadata().remove("values"));
            });
            return dictionary;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new TreeMap<>();
    }

    public Map<String, Map> getFENCEMapping(){
        if(_projectMap == null || _projectMap.isEmpty()) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                // Use the fenceMapping Resource to read the JSON file and convert it to a Map
                Map<String, Object> jsonMap = objectMapper.readValue(fenceMapping.getInputStream(), Map.class);
                List<Map> projects = (List<Map>) jsonMap.get("bio_data_catalyst");
                _projectMap = new HashMap<String, Map>(projects.size());
                for(Map project : projects) {
                    String consentVal = (project.get("consent_group_code") != null && project.get("consent_group_code") != "") ?
                            "" + project.get("study_identifier") + "." + project.get("consent_group_code") :
                            "" + project.get("study_identifier");
                    _projectMap.put(consentVal, project);
                }

            } catch (Exception e) {
                return new HashMap<String,Map>();
            }
        }

        return _projectMap;
    }

    private Map getValueThatContainsKey(String tag) {
        Map result = null;
        if (getFENCEMapping() != null) {
            for (String key : getFENCEMapping().keySet()) {
                if (key.toUpperCase().contains(tag)) {
                    result = getFENCEMapping().get(key);
                    break; // Stop iterating after the first match is found
                }
            }
        }
        return result;
    }

    public void setFenceMapping(Resource fenceMapping) {
        this.fenceMapping = fenceMapping;
    }
}
