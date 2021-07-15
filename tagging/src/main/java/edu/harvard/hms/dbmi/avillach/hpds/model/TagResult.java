package edu.harvard.hms.dbmi.avillach.hpds.model;


import lombok.Data;

@Data
public class TagResult {
    private final String tag;
    private final int score;
}
