package edu.harvard.hms.dbmi.avillach.hpds;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.map.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

@Path("/")
public class HelloWorld {

	private static final String TMP_DICTIONARY_JAVABIN = "/tmp/dictionary.javabin";
	private TreeMap<String, TopmedDataTable> fhsDictionary;

	public HelloWorld() throws IOException {
		fhsDictionary = new TreeMap<>();

		//		if(! new File(TMP_DICTIONARY_JAVABIN).exists()) {

		for(File studyFolder : new File("/Users/jason/avl/2021/tagging/dicts/").listFiles()) {
			if(studyFolder!=null) {
				Arrays.stream(new File(studyFolder, "rawData")
					.list((file, name)->{
						return name.endsWith("data_dict.xml");}
							)).forEach((table)->{
								TopmedDataTable topmedDataTable;
								try {
									topmedDataTable = loadDataTable(studyFolder.getAbsolutePath()+"/rawData/"+table);
									fhsDictionary.put(topmedDataTable.metadata.get("id"), topmedDataTable);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							});
			}
		}

		for(File studyFolder : new File("/Users/jason/avl/2021/tagging/dicts/").listFiles()) {
			Arrays.stream(new File(studyFolder, "rawData").list((file, name)->{
				return name.endsWith("var_report.xml");}
					)).forEach((table)->{
						String[] split = table.split("\\.");
						if(split.length > 2) {
							String tableId = split[2] + "." + split[3];
							System.out.println(tableId);
							TopmedDataTable topmedDataTable = fhsDictionary.get(tableId);
							if(topmedDataTable!=null) {
								try {
									Document doc = Jsoup.parse(new File(studyFolder.getAbsolutePath()+"/rawData/"+table), "UTF-8");
									topmedDataTable.loadVarReport(doc);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					});
		}
		TreeSet<String> tags = new TreeSet<>();
		for(TopmedDataTable table : fhsDictionary.values()) {
			Collection<TopmedVariable> variables = table.variables.values();
			for(TopmedVariable variable : variables) {
				tags.addAll(variable.getMetadata_tags());
				tags.addAll(variable.getValue_tags());
			}
		}

		//			try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(TMP_DICTIONARY_JAVABIN));){
		//				oos.writeObject(fhsDictionary);
		//				oos.flush();
		//				oos.close();
		//			}

		//		}else {
		//			try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(TMP_DICTIONARY_JAVABIN));){
		//				fhsDictionary = (TreeMap<String, TopmedDataTable>) ois.readObject();
		//			} catch (ClassNotFoundException e) {
		//				// TODO Auto-generated catch block
		//				e.printStackTrace();
		//			}
		//		}


	}

	private TopmedDataTable loadDataTable(String pathname) throws IOException {
		Document doc = Jsoup.parse(new File(pathname), "UTF-8");
		TopmedDataTable topmedDataTable = new TopmedDataTable(doc);
		return topmedDataTable;
	}



	@GET
	@Path("/search/{input}")
	@Produces("application/json")
	@Consumes("text/plain")
	public Response search(@PathParam("input") String input) {
		Map<Double, List<TopmedVariable>> results = new TreeMap<>();
		TreeMap<String,Integer> tagStats = new TreeMap<String, Integer>();
		TreeSet<String> tags = new TreeSet<>();
		TreeMap<String, TreeMap<String, TreeMap<String, String>>> studies = new TreeMap<String, TreeMap<String, TreeMap<String,String>>>();
		for(TopmedDataTable table : fhsDictionary.values()) {
			TreeMap<String, TreeMap<String, String>> dataTables = studies.get(table.metadata.get("study_id"));
			if(dataTables==null) {
				dataTables = new TreeMap<String, TreeMap<String, String>>();
				studies.put(table.metadata.get("study_id"), dataTables);
			}
			Map<Double, List<TopmedVariable>> search = table.search(input);
			for(Double score : search.keySet()) {
				List<TopmedVariable> resultsForScore = results.get(score);
				if(resultsForScore==null) {
					resultsForScore = new ArrayList<TopmedVariable>();
				}
				resultsForScore.addAll(search.get(score).stream().map((result)->{
					result.getMetadata().put("dataTableId", table.metadata.get("id"));
					return result;
				}).collect(Collectors.toList()));
				results.put(score, resultsForScore);
				for(TopmedVariable variable : resultsForScore) {
					tags.addAll(variable.getMetadata_tags());
					tags.addAll(variable.getValue_tags());
				}
			}
			for(String tag : tags) {
				tagStats.put(tag, 0);
			}

			if(search.values().stream().flatMap(Collection::stream)
					.collect(Collectors.toList()).size()>0) {
				dataTables.put(table.metadata.get("id"), table.metadata);
			}
			if(dataTables.isEmpty()) {
				studies.remove(table.metadata.get("study_id"));
			}
		}
		for(List<TopmedVariable> scoreResults : results.values()) {
			for(TopmedVariable result : scoreResults) {

				for(String tag : result.getMetadata_tags()) {
					tagStats.put(tag,tagStats.get(tag)+1);
				}
				for(String tag : result.getValue_tags()) {
					tagStats.put(tag,tagStats.get(tag)+1);
				}
			}
		}
		int numberOfVariables = 0;
		for(List<TopmedVariable> resultsForScore : results.values()) {
			numberOfVariables += resultsForScore.size();
		}
		final int numVars = numberOfVariables;
		if(tagStats.size()>10) {
			tagStats = new TreeMap<>(tagStats.entrySet().stream().filter((Map.Entry<String, Integer> entry)->{
				return entry.getValue() > numVars * .05 && entry.getValue() < numVars * .95;
			}).collect(Collectors.toMap(
					(Map.Entry<String, Integer> entry)->{return entry.getKey();}, 
					(Map.Entry<String, Integer> entry)->{return entry.getValue();})));			
		}

		String firstStudy = studies.firstKey();
		results = (Map<Double, List<TopmedVariable>>) results.entrySet().stream().map((entry)->{
			return new SimpleEntry(entry.getKey(), entry.getValue().stream().filter((variable)->{
				return true;//variable.studyId.equalsIgnoreCase(firstStudy);
			}).collect(Collectors.toList()));}).collect(Collectors.toMap((entry)->{return (Double)entry.getKey();},(entry)->{
				return (List<TopmedVariable>)entry.getValue();}));
		;

		return Response.ok(Map.of(
				"studies", studies,
				"dataTables", fhsDictionary.entrySet().parallelStream().filter((table)->{
					return studies.keySet().contains(table.getValue().metadata.get("study_id"));
				}).collect(Collectors.toMap(
						(table)->{return table.getValue().metadata.get("id");}, 
						(table)->{return table.getValue().metadata.get("description");
						})),
				"tags", tagStats,
				"results", results)).build();
	}

	@GET
	@Path("/search/{input}/results/{study}")
	@Produces("application/json")
	@Consumes("text/plain")
	public Response results(@PathParam("input") String input, @PathParam("study") String study) {
		Map<Double, List<TopmedVariable>> results = new TreeMap<>();
		for(TopmedDataTable table : fhsDictionary.values()) {
			Map<Double, List<TopmedVariable>> search = table.search(input);
			for(Double score : search.keySet()) {
				List<TopmedVariable> resultsForScore = results.get(score);
				if(resultsForScore==null) {
					resultsForScore = new ArrayList<TopmedVariable>();
				}
				resultsForScore.addAll(search.get(score).stream().map((result)->{
					result.getMetadata().put("dataTableId", table.metadata.get("id"));
					return result;
				}).collect(Collectors.toList()));
				results.put(score, resultsForScore);
			}
		}

		return Response.ok(Map.of(
				"results", results)).build();
	}

	@POST
	@Produces("application/json")
	@Consumes("application/json")
	@Path("/jsonBean")
	public Response modifyJson(JsonBean input) {
		input.setVal2(input.getVal1());
		return Response.ok().entity(input).build();
	}
}



