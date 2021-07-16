package edu.harvard.hms.dbmi.avillach.hpds.model.domain;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class Query {

    private String id;
    private EntityType entityType;
}
