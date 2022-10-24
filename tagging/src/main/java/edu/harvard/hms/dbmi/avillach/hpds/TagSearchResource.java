package edu.harvard.hms.dbmi.avillach.hpds;

import edu.harvard.hms.dbmi.avillach.hpds.model.*;
import edu.harvard.hms.dbmi.avillach.hpds.model.domain.*;
import lombok.SneakyThrows;

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

    private TreeMap<String, TopmedDataTable> fhsDictionary;
    private static final String JAVABIN = "/usr/local/docker-config/search/dictionary.javabin";
    private static final int INITIAL_RESULTS_SIZE = 20;

    public TagSearchResource() {
        fhsDictionary = readDictionary();
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
                	(result.getScore() > numVars * .05 && result.getScore() < numVars * .95)
                ));
            }
        }

        // flatten the results for each score into a list of search results
        List<SearchResult> searchResults = results.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(topmedVariable -> {
                            if (topmedVariable.getValues().size() > 0 && searchQuery.getVariableValuesLimit() != null && searchQuery.getVariableValuesLimit() > 0) {
                                return topmedVariable.copy(searchQuery.getVariableValuesLimit());
                            }
                            return topmedVariable;
                        })
                        .map(topmedVariable -> new SearchResult(topmedVariable, entry.getKey())))
                .sorted(Comparator.comparing(SearchResult::getScore).reversed().thenComparing(result -> result.getResult().getMetadata().get("columnmeta_description")))
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


    private TreeMap<String, TopmedDataTable> readDictionary() {
        try(ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(JAVABIN)));){
            TreeMap<String, TopmedDataTable> dictionary = (TreeMap<String, TopmedDataTable>) ois.readObject();
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
}
