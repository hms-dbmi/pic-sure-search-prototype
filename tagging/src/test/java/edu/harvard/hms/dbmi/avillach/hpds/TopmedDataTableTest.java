package edu.harvard.hms.dbmi.avillach.hpds;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

public class TopmedDataTableTest {

	@Test
	public void loadDataTableTest() throws IOException {
		HashMap<String, TopmedDataTable> fhsDictionary = new HashMap<>();
		HashMap<String,Integer> tagStats = new HashMap<>();
		Arrays.stream(new File("/Users/jason/avl/2021/tagging/dicts/fhs").list()).forEach((table)->{
			TopmedDataTable topmedDataTable;
			try {
				topmedDataTable = loadDataTable("/Users/jason/avl/2021/tagging/dicst/fhs/" + table, tagStats);
				topmedDataTable.metadata.put("Study", "FHS");
				fhsDictionary.put(topmedDataTable.metadata.get("id"), topmedDataTable);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
		Arrays.stream(new File("/Users/jason/avl/2021/tagging/dicst/copdgene").list()).forEach((table)->{
			TopmedDataTable topmedDataTable;
			try {
				topmedDataTable = loadDataTable("/Users/jason/avl/2021/tagging/dicts/copdgene/" + table, tagStats);
				topmedDataTable.metadata.put("Study", "COPDgene");
				fhsDictionary.put(topmedDataTable.metadata.get("id"), topmedDataTable);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
		ObjectMapper mapper = new ObjectMapper();
		Map<Double, List<TopmedVariable>> results = new HashMap<>();
		for(TopmedDataTable table : fhsDictionary.values()) {
			Map<Double, List<TopmedVariable>> search = table.search("smoke");
			for(Double score : search.keySet()) {
				List<TopmedVariable> resultsForScore = results.get(score);
				if(resultsForScore==null) {
					resultsForScore = new ArrayList<TopmedVariable>();
				}
				resultsForScore.addAll(search.get(score));
				results.put(score, resultsForScore);
			}
		}
		HashMap<String,Integer> tagStats2 = new HashMap<>(tagStats.entrySet().stream().filter((Map.Entry<String, Integer> entry)->{
			return entry.getValue() > 5;
		}).collect(Collectors.toMap(
				(Map.Entry<String, Integer> entry)->{return entry.getKey();}, 
				(Map.Entry<String, Integer> entry)->{return entry.getValue();})));
	}
	@Test
	public void hellowWorldSearchTest() throws IOException {
		Object results = new HelloWorld().search("heart").getEntity();
		System.out.println(new ObjectMapper().writeValueAsString(
				results));

	}

	private TopmedDataTable loadDataTable(String pathname, HashMap<String, Integer> tagStats) throws IOException {
		Document doc = Jsoup.parse(new File(pathname), "UTF-8");
		TopmedDataTable topmedDataTable = new TopmedDataTable(doc);
		
		return topmedDataTable;
	}
}
