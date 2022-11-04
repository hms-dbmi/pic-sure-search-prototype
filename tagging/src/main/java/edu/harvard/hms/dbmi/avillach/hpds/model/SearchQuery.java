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
    private final boolean returnTags;
    private final int offset, limit;
    /**
     * Parameter to limit variable values returned. Using the following rules:
     * NULL: return all variable values
     * 0: return no variable values
     * n > 0: return at most n variable values
     */
    private final Integer variableValuesLimit;
}
