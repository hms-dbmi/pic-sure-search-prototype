package edu.harvard.hms.dbmi.avillach.hpds;

import com.fasterxml.jackson.databind.ObjectMapper;
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

@Path("/pic-sure")
@Produces({"application/json"})
@Consumes({"application/json"})
public class TagSearchResource implements IResourceRS {

    private TreeMap<String, TopmedDataTable> fhsDictionary;
    private static final String TMP_DICTIONARY_JAVABIN = "/tmp/dictionary.javabin";
    private static final int INITIAL_RESULTS_SIZE = 20;
    private final ObjectMapper mapper = new ObjectMapper();

    public TagSearchResource() {
        fhsDictionary = readDictionary();
    }


    @Override
    public ResourceInfo info(QueryRequest queryRequest) {
        return null;
    }

    @SneakyThrows
    @Override
    @POST
    @Path("/search")
    public SearchResults search(SearchRequest searchRequest) {
        final SearchQuery searchQuery = searchRequest.getQuery();
        Map<Double, List<TopmedVariable>> results = new TreeMap<>();
        TreeMap<String,Integer> tagStats = new TreeMap<String, Integer>();
        TreeSet<String> tags = new TreeSet<>();
        for(TopmedDataTable table : fhsDictionary.values()) {
            Collection<TopmedVariable> variablesMatchingTags;
            if (searchQuery.getIncludedTags() != null) {
                variablesMatchingTags = table.variables.values().stream().filter(variable -> {
                    for (String includedTag : searchQuery.getIncludedTags()) {
                        if (variable.relevance(includedTag) == 0)
                            return false;
                    }
                    return true;
                }).collect(Collectors.toList());
            } else {
                variablesMatchingTags = table.variables.values();
            }

            Map<Double, List<TopmedVariable>> search = TopmedDataTable.searchVariables(searchQuery.getSearchTerm(), variablesMatchingTags);
            if (search.size() == 0) {
                continue;
            }

            for(Double score : search.keySet()) {
                List<TopmedVariable> resultsForScore = results.get(score);
                if(resultsForScore==null) {
                    resultsForScore = new ArrayList<>();
                }
                resultsForScore.addAll(search.get(score).stream().map((result)->{
                    result.getMetadata().put("dataTableId", table.metadata.get("id"));
                    return result;
                }).collect(Collectors.toList()));
                results.put(score, resultsForScore);

                if (searchQuery.isReturnTags()) {
                    for(TopmedVariable variable : resultsForScore) {
                        tags.addAll(variable.getMetadata_tags());
                        tags.addAll(variable.getValue_tags());
                    }
                }
            }
            for(String tag : tags) {
                tagStats.put(tag, 0);
            }
        }

        if (searchQuery.isReturnTags()) {
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
        }
        int numberOfVariables = 0;
        for(List<TopmedVariable> resultsForScore : results.values()) {
            numberOfVariables += resultsForScore.size();
        }
        final int numVars = numberOfVariables;

        List<SearchResult> searchResults = results.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(topmedVariable -> new SearchResult(topmedVariable, entry.getKey())))
                .sorted(Comparator.comparing(SearchResult::getScore).reversed().thenComparing(result -> result.getResult().getMetadata().get("description")))
                .collect(Collectors.toList());

        Stream<TagResult> tagResults = Stream.empty();
        if (searchQuery.isReturnTags()) {
            tagResults = tagStats.entrySet().stream()
                    .map(entry -> new TagResult(entry.getKey(), entry.getValue()));
            if(tagStats.size()>10) {
                tagResults = tagResults.filter(result -> result.getScore() > numVars * .05 && result.getScore() < numVars * .95);
            }
            tagResults = tagResults.sorted(Comparator.comparing(TagResult::getScore).reversed());
        }

        int searchResultsSize = searchResults.size();
        if (!searchRequest.getQuery().isReturnAllResults()) {
            searchResults = searchResults.subList(0, Math.min(INITIAL_RESULTS_SIZE, searchResults.size() - 1));
        }
        TagSearchResponse tagSearchResponse = new TagSearchResponse(
                tagResults.collect(Collectors.toList()),
                searchResults,
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

    private Response getDataTable(String id) {
        TopmedDataTable topmedDataTable = fhsDictionary.get(String.valueOf(id));
        return Response.ok(topmedDataTable).build();
    }

    @Override
    public Response queryFormat(QueryRequest resultRequest) {
        return null;
    }


    private TreeMap<String, TopmedDataTable> readDictionary() {
        try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(TMP_DICTIONARY_JAVABIN));){
            return (TreeMap<String, TopmedDataTable>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new TreeMap<>();
    }
}
