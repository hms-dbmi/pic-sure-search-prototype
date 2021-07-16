package edu.harvard.hms.dbmi.avillach.hpds.model.domain;

import edu.harvard.hms.dbmi.avillach.hpds.model.SearchQuery;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
@Jacksonized
public class SearchRequest {

    private Map<String, String> resourceCredentials;

    private final SearchQuery query;

    private final UUID resourceUUID;
}
