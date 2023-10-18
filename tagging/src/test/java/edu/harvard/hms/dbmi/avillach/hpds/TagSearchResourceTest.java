package edu.harvard.hms.dbmi.avillach.hpds;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.*;
import edu.harvard.hms.dbmi.avillach.hpds.model.SearchQuery;
import edu.harvard.hms.dbmi.avillach.hpds.model.TagSearchResponse;
import edu.harvard.hms.dbmi.avillach.hpds.model.domain.SearchRequest;
import edu.harvard.hms.dbmi.avillach.hpds.model.domain.SearchResults;
import org.junit.Before;
import org.junit.Test;
import java.io.ByteArrayInputStream;
import org.springframework.core.io.ByteArrayResource;
import java.util.Map;

import static org.junit.Assert.assertEquals;

import java.util.*;

import static org.junit.Assert.*;

import org.springframework.core.io.Resource;

public class TagSearchResourceTest {

    private TagSearchResource tagSearchResource;

    /**
     * These are some basic mock objects, minimally populated. It should be safe to more fully flesh out these objects
     * without breaking existing tests, although adding more TopmedVariable or TopmedDataTable may not be safe.
     */
    @Before
    public void setUp() {
        SortedMap<String, TopmedDataTable> dictionary = new TreeMap<>();

        TopmedVariable variable1 = new TopmedVariable()
                .setVarId("var1")
                .setMetadata(ImmutableMap.of("columnmeta_description", "Chest Pain"));
        variable1.allTagsLowercase = ImmutableSet.of("heart", "chest", "pain");

        TopmedVariable variable2 = new TopmedVariable()
                .setVarId("var2")
                .setMetadata(ImmutableMap.of("columnmeta_description", "Afib"));
        variable2.allTagsLowercase = ImmutableSet.of("heart", "attack", "afib");

        TopmedVariable variable3 = new TopmedVariable()
                .setVarId("var3")
                .setMetadata(ImmutableMap.of("columnmeta_description", "Hearing loss"));
        variable3.allTagsLowercase = ImmutableSet.of("hear", "hearing");

        TopmedDataTable dataTable1 = new TopmedDataTable();
        dataTable1.variables = ImmutableSortedMap.of(
                "var1", variable1,
                "var2", variable2,
                "var3", variable3
        );


        TopmedVariable variable4 = new TopmedVariable()
                .setVarId("var4")
                .setMetadata(ImmutableMap.of("columnmeta_description", "Height"));
        variable4.allTagsLowercase = ImmutableSet.of("height");
        TopmedDataTable dataTable2 = new TopmedDataTable();

        TopmedVariable variable5 = new TopmedVariable()
                .setVarId("var5")
                .setMetadata(ImmutableMap.of("columnmeta_description", "Blood Type"))
                .setValues(ImmutableList.of("A","B", "AB","O"));
        variable5.allTagsLowercase = ImmutableSet.of("blood", "type", "heart");
        dataTable2.variables = ImmutableSortedMap.of(
                "var4", variable4,
                "var5", variable5
        );

        dictionary.put("dataTable1", dataTable1);
        dictionary.put("dataTable2", dataTable2);

        tagSearchResource = new TagSearchResource(dictionary);
    }

    @Test
    public void basicSearch() {
        SearchRequest.SearchRequestBuilder builder = SearchRequest.builder();
        builder.resourceUUID(UUID.randomUUID());
        SearchQuery.SearchQueryBuilder searchQueryBuilder = SearchQuery.builder();
        searchQueryBuilder.searchTerm("heart");
        searchQueryBuilder.excludedTags(List.of());
        searchQueryBuilder.includedTags(List.of());
        searchQueryBuilder.offset(0);
        searchQueryBuilder.returnTags(false);
        builder.query(searchQueryBuilder.build());
        SearchResults searchResults = tagSearchResource.search(builder.build());

        assertEquals(searchResults.getResults().getClass(), TagSearchResponse.class);
        TagSearchResponse tagSearchResponse = (TagSearchResponse) searchResults.getResults();

        assertEquals(tagSearchResponse.getSearchResults().size(), 3);
        assertTrue(Ordering.from(TagSearchResource.SEARCH_RESULT_COMPARATOR).isOrdered(tagSearchResponse.getSearchResults()));
        assertTrue(tagSearchResponse.getTags().isEmpty());
    }

    @Test
    public void basicSearchWithTags() {
        SearchRequest.SearchRequestBuilder builder = SearchRequest.builder();
        builder.resourceUUID(UUID.randomUUID());
        SearchQuery.SearchQueryBuilder searchQueryBuilder = SearchQuery.builder();
        searchQueryBuilder.searchTerm("heart");
        searchQueryBuilder.excludedTags(List.of());
        searchQueryBuilder.includedTags(List.of());
        searchQueryBuilder.offset(0);
        searchQueryBuilder.returnTags(true);
        builder.query(searchQueryBuilder.build());
        SearchResults searchResults = tagSearchResource.search(builder.build());

        assertEquals(searchResults.getResults().getClass(), TagSearchResponse.class);
        TagSearchResponse tagSearchResponse = (TagSearchResponse) searchResults.getResults();

        assertEquals(tagSearchResponse.getSearchResults().size(), 3);
        assertTrue(Ordering.from(TagSearchResource.SEARCH_RESULT_COMPARATOR).isOrdered(tagSearchResponse.getSearchResults()));
    }

    @Test
    public void searchVerifyJson() throws JsonProcessingException {
        SearchRequest.SearchRequestBuilder builder = SearchRequest.builder();
        builder.resourceUUID(UUID.randomUUID());
        SearchQuery.SearchQueryBuilder searchQueryBuilder = SearchQuery.builder();
        searchQueryBuilder.searchTerm("heart");
        searchQueryBuilder.excludedTags(List.of());
        searchQueryBuilder.includedTags(List.of());
        searchQueryBuilder.offset(0);
        searchQueryBuilder.returnTags(true);
        builder.query(searchQueryBuilder.build());
        SearchResults searchResults = tagSearchResource.search(builder.build());

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(searchResults);
        assertFalse(json.contains("\"tagMap\":"));
    }


    @Test
    public void searchPartialMatch() {
        SearchRequest.SearchRequestBuilder builder = SearchRequest.builder();
        builder.resourceUUID(UUID.randomUUID());
        SearchQuery.SearchQueryBuilder searchQueryBuilder = SearchQuery.builder();
        searchQueryBuilder.searchTerm("hear");
        searchQueryBuilder.excludedTags(List.of());
        searchQueryBuilder.includedTags(List.of());
        searchQueryBuilder.offset(0);
        searchQueryBuilder.returnTags(true);
        builder.query(searchQueryBuilder.build());
        SearchResults searchResults = tagSearchResource.search(builder.build());

        assertEquals(searchResults.getResults().getClass(), TagSearchResponse.class);
        TagSearchResponse tagSearchResponse = (TagSearchResponse) searchResults.getResults();

        assertEquals(tagSearchResponse.getSearchResults().size(), 4);
        // there should only be one exact match, which has a score of more than 100
        assertEquals(tagSearchResponse.getSearchResults().stream().filter(searchResult -> searchResult.getScore() >= 100).count(), 1);

        assertTrue(Ordering.from(TagSearchResource.SEARCH_RESULT_COMPARATOR).isOrdered(tagSearchResponse.getSearchResults()));
        assertNotNull(tagSearchResponse.getTags());
    }

    @Test
    public void searchLimitVariableValues1() {
        SearchRequest.SearchRequestBuilder builder = SearchRequest.builder();
        builder.resourceUUID(UUID.randomUUID());
        SearchQuery.SearchQueryBuilder searchQueryBuilder = SearchQuery.builder();
        searchQueryBuilder.searchTerm("blood");
        searchQueryBuilder.excludedTags(List.of());
        searchQueryBuilder.includedTags(List.of());
        searchQueryBuilder.offset(0);
        searchQueryBuilder.returnTags(false);
        searchQueryBuilder.variableValuesLimit(1);
        builder.query(searchQueryBuilder.build());
        SearchResults searchResults = tagSearchResource.search(builder.build());

        assertEquals(searchResults.getResults().getClass(), TagSearchResponse.class);
        TagSearchResponse tagSearchResponse = (TagSearchResponse) searchResults.getResults();

        tagSearchResponse.getSearchResults().forEach(searchResult -> {
            assertEquals(searchResult.getResult().getValues().size(), 1);
        });
    }

    @Test
    public void searchLimitVariableValues0() {
        SearchRequest.SearchRequestBuilder builder = SearchRequest.builder();
        builder.resourceUUID(UUID.randomUUID());
        SearchQuery.SearchQueryBuilder searchQueryBuilder = SearchQuery.builder();
        searchQueryBuilder.searchTerm("blood");
        searchQueryBuilder.excludedTags(List.of());
        searchQueryBuilder.includedTags(List.of());
        searchQueryBuilder.offset(0);
        searchQueryBuilder.returnTags(false);
        searchQueryBuilder.variableValuesLimit(0);
        builder.query(searchQueryBuilder.build());
        SearchResults searchResults = tagSearchResource.search(builder.build());

        assertEquals(searchResults.getResults().getClass(), TagSearchResponse.class);
        TagSearchResponse tagSearchResponse = (TagSearchResponse) searchResults.getResults();

        tagSearchResponse.getSearchResults().forEach(searchResult -> {
            assertEquals(searchResult.getResult().getValues().size(), 0);
        });
    }

    @Test
    public void searchLimitVariableValuesNull() {
        SearchRequest.SearchRequestBuilder builder = SearchRequest.builder();
        builder.resourceUUID(UUID.randomUUID());
        SearchQuery.SearchQueryBuilder searchQueryBuilder = SearchQuery.builder();
        searchQueryBuilder.searchTerm("blood");
        searchQueryBuilder.excludedTags(List.of());
        searchQueryBuilder.includedTags(List.of());
        searchQueryBuilder.offset(0);
        searchQueryBuilder.returnTags(false);
        builder.query(searchQueryBuilder.build());
        SearchResults searchResults = tagSearchResource.search(builder.build());

        assertEquals(searchResults.getResults().getClass(), TagSearchResponse.class);
        TagSearchResponse tagSearchResponse = (TagSearchResponse) searchResults.getResults();

        // No variableValues specified should return all values
        tagSearchResponse.getSearchResults().forEach(searchResult -> {
            assertEquals(searchResult.getResult().getValues().size(), 4);
        });
    }

    @Test
    public void testGetFENCEMappingWithValidJson() throws Exception {
        // Define your JSON content
        String json = "{\"bio_data_catalyst\":[{\"study_identifier\":\"Study1\",\"consent_group_code\":\"Consent1\"}]}";

        // Create a ByteArrayResource with the JSON content (for testing purposes)
        Resource resource = new ByteArrayResource(json.getBytes());

        // Set the fenceMapping Resource directly (for testing purposes)
        tagSearchResource.setFenceMapping(resource);

        Map<String, Map> result = tagSearchResource.getFENCEMapping();

        Map<String, Map> expected = new HashMap<>();
        Map<String, Object> project = new HashMap<>();
        project.put("study_identifier", "Study1");
        project.put("consent_group_code", "Consent1");
        expected.put("Study1.Consent1", project);

        assertEquals(expected, result);
    }

    @Test
    public void testGetFENCEMappingWithInvalidJson() throws Exception {
        // Define invalid JSON content
        String invalidJson = "Invalid JSON";

        // Create a ByteArrayResource with the invalid JSON content (for testing purposes)
        Resource resource = new ByteArrayResource(invalidJson.getBytes());

        // Set the fenceMapping Resource directly (for testing purposes)
        tagSearchResource.setFenceMapping(resource);

        Map<String, Map> result = tagSearchResource.getFENCEMapping();

        assertEquals(new HashMap<>(), result);
    }

    @Test
    public void testGetFENCEMappingWithEmptyJson() throws Exception {
        // Define an empty JSON content
        String emptyJson = "{}";

        // Create a ByteArrayResource with the empty JSON content (for testing purposes)
        Resource resource = new ByteArrayResource(emptyJson.getBytes());

        // Set the fenceMapping Resource directly (for testing purposes)
        tagSearchResource.setFenceMapping(resource);

        Map<String, Map> result = tagSearchResource.getFENCEMapping();

        assertEquals(new HashMap<>(), result);
    }
}