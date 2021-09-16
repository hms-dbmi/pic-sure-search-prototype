package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class HPDSPathLookup {
	private HashMap<String,String> mappings = new HashMap<>();
	
	public HashMap<String, String> getMappings() {
		return mappings;
	}

	public void setMappings(HashMap<String, String> mappings) {
		this.mappings = mappings;
	}

	public HPDSPathLookup() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("/tmp/concept_mapping.csv"));
		String line = reader.readLine();
		while(line!=null) {
			String[] entries = Arrays.stream(line.split("\",\"")).filter((value)->{return ! value.isEmpty();}).collect(Collectors.toList()).toArray(new String[0]);
			mappings.put(entries[1].replaceAll("\"", ""),entries[0].replaceAll("\"", ""));
			line = reader.readLine();
		}
		
	}
}
