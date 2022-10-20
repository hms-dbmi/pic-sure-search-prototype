package edu.harvard.hms.dbmi.avillach.hpds.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Data
@Jacksonized
@Builder
public class SearchQuery {

    private final String searchTerm;
    private final List<String> includedTags;
    private final List<String> excludedTags;
    private final boolean returnTags, excludeVariableValues;
    private final int offset, limit;
}
