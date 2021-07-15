package edu.harvard.hms.dbmi.avillach.hpds.model;

import lombok.Data;

import java.util.List;

@Data
public class TagSearchResponse {
    private final List<TagResult> tags;
    private final List<SearchResult> searchResults;
}
