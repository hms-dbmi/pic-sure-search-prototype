package edu.harvard.hms.dbmi.avillach.hpds.model;


import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import lombok.Data;

@Data
public class SearchResult {

    private final TopmedVariable result;
    private final Double score;
}
