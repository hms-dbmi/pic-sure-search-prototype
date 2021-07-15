package edu.harvard.hms.dbmi.avillach.hpds.model;

import lombok.Data;

import java.util.List;

@Data
public class QueryResponse {

    private final List<SearchResult> searchResults;
}
