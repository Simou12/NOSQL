package qengine.program;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import riotcmd.infer;

final class Main {
	static final String baseURI = null;

	static final String workingDir = "data/";

	static String queryFile = workingDir + "STAR_ALL_workload.queryset", queryFolder, outputDir = "output/",
			csvFile = outputDir + "csvfile.csv", statscsv = outputDir + "statscsv.csv";
	// STAR_ALL_workload
	static String dataFile = workingDir + "100K.nt";

	static HashSet<String> queryFiles;

	static boolean useFolder = false, useJena = false, shuffle = false, exportResultCSV=false;

	static int warm = 0, nbQueriesSameNbPatterns = 0, nbRequeteDouble = 0, nbPatterns = 0, nbQueriesPerSeconde = 0;

	static long startTimePattern = 0, endTimePattern = 0, ramTotalPattern = 0;
	static double ramUtilisePattern = 0;

	static Dictionnary dictionnary;

	static Index index;

	private static Set<String> results = new HashSet<>();

	private static Set<String> allResults = new HashSet<>();

	private static Set<String> resultsJena = new HashSet<>();

	public static Map<Integer, Integer> sameNBPatternsMap = new HashMap<>();

	public static Map<Integer, Long> timeTotPerPatternMap = new HashMap<>();

	public static Map<Integer, Double> ramTotPerPatternMap = new HashMap<>();
	
	public static Map<Integer, Integer>  nbResponsePerNbQuery= new HashMap<>();
	
	public static int nbqueries = 0;
	
	public static HashSet<String> processJENA(String sparqlQueryString, Model model) {
		// Create a SPARQL query
		Query query = QueryFactory.create(sparqlQueryString);

		// Create a QueryExecution object to execute the query on the given model
		try (QueryExecution queryExecution = QueryExecutionFactory.create(query, model)) {
			// Execute the query and obtain the result set
			ResultSet resultSet = queryExecution.execSelect();

			// Process and print the results
			HashSet<String> results = new HashSet<String>();

			while (resultSet.hasNext()) {
				QuerySolution solution = resultSet.nextSolution();

				results.add(solution.get("?v0").toString());
			}
			return results;
		}
	}

	private static MainRDFHandler parseData() throws FileNotFoundException, IOException {

		try (Reader dataReader = new FileReader(dataFile)) {

			// On va parser des données au format ntriples
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
			MainRDFHandler mainRdfHandler = new MainRDFHandler();
			// On utilise notre implémentation de handler
			rdfParser.setRDFHandler(mainRdfHandler);
			// Parsing et traitement de chaque triple par le handler
			rdfParser.parse(dataReader, baseURI);

			dictionnary = mainRdfHandler.getDictionnary();
			index = mainRdfHandler.getIndex();
			return (mainRdfHandler);
		}
	}

	private static List<ParsedQuery> parseQueries(String queryFilePath) throws FileNotFoundException, IOException {
		List<ParsedQuery> allQueries = new ArrayList<>();
		StringBuilder queryString = new StringBuilder();
		SPARQLParser sparqlParser = new SPARQLParser();
		try (Stream<String> lineStream = Files.lines(Paths.get(queryFilePath))) {
			lineStream.forEach(line -> {
				queryString.append(line);
				if (line.trim().endsWith("}")) {
					allQueries.add(sparqlParser.parseQuery(queryString.toString(), baseURI));
					queryString.setLength(0);
				}
			});
		}
		return allQueries;
	}

	private static List<ParsedQuery> parseQueriesDirectory(String directory) throws IOException {
		List<ParsedQuery> allQueriesAllFiles = new ArrayList<>();
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(queryFolder), "*.queryset")) {
			for (Path entry : stream) {
				String queryFilePath = entry.toString();
				List<ParsedQuery> allQueries = parseQueries(queryFilePath);
				allQueriesAllFiles.addAll(allQueries);
			}
		}
		return allQueriesAllFiles;
	}

	public static Set<String> processAQuery(ParsedQuery query) {
		int nbReponses=0;
		nbqueries++;
		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
		startTimePattern = System.currentTimeMillis();
		Runtime runtime = Runtime.getRuntime();
		ramTotalPattern = runtime.totalMemory() - runtime.freeMemory();
		nbPatterns = patterns.size();
		// Nombre de patterns par requêtes
		sameNBPatternsMap.put(nbPatterns, sameNBPatternsMap.getOrDefault(nbPatterns, 0) + 1);
		java.util.Map<Integer, java.util.Map<Integer, HashSet<Integer>>> pos = index.getPos();
		HashSet<Integer> result;
		Set<String> text_result = new HashSet<String>();
		int indexOb = dictionnary.getKey(patterns.get(0).getObjectVar().getValue().toString());
		int indexPred = dictionnary.getKey(patterns.get(0).getPredicateVar().getValue().toString());
		if (indexOb == -1 || indexPred == -1)
			return text_result;
		if (pos.containsKey(indexPred)) {
			java.util.Map<Integer, HashSet<Integer>> dicPredicate = pos.get(indexPred);
			if (dicPredicate.containsKey(indexOb))
				result = (HashSet) dicPredicate.get(indexOb).clone();
			else
				return text_result;
		} else
			return text_result;

		patterns.remove(0);

		// all other patterns
		for (StatementPattern pattern : patterns) {
			indexOb = dictionnary.getKey(pattern.getObjectVar().getValue().toString());
			indexPred = dictionnary.getKey(pattern.getPredicateVar().getValue().toString());
			if (pos.containsKey(indexPred)) {
				java.util.Map<Integer, HashSet<Integer>> dicPredicate = pos.get(indexPred);
				if (dicPredicate.containsKey(indexOb))
					result.retainAll(dicPredicate.get(indexOb));
				else
					return text_result;
			} else
				return text_result;
		}
		// int --> String
		text_result = result.stream().map(dictionnary.getDictionnary()::get).collect(Collectors.toSet());
		ramUtilisePattern = (double) (runtime.totalMemory() - runtime.freeMemory() - ramTotalPattern) / (1000000000.00);
		ramTotPerPatternMap.put(nbPatterns,
				ramTotPerPatternMap.getOrDefault(nbPatterns, (double) 0.0) + ramUtilisePattern);
		endTimePattern = System.currentTimeMillis() - startTimePattern;
		timeTotPerPatternMap.put(nbPatterns,
				timeTotPerPatternMap.getOrDefault(nbPatterns, (long) 0.0) + endTimePattern);
		nbReponses=text_result.size();
		nbResponsePerNbQuery.put(nbReponses,
				nbResponsePerNbQuery.getOrDefault(nbReponses, 0)+1);
		return text_result;
	}

	private static List<Set<String>> processQueries(List<ParsedQuery> requettes)
			throws FileNotFoundException, IOException {
		List<String> allQueries = requettes.stream().map(s -> s.getSourceString().replaceAll("\\s", ""))
				.collect(Collectors.toList());
		Set<String> reqSansDoublons = new HashSet<>(allQueries);
		nbRequeteDouble = allQueries.size() - reqSansDoublons.size();
		System.out.println();
		System.out.println(nbRequeteDouble);
		List<Set<String>> all_results = new ArrayList<Set<String>>();
		Set<String> results = new HashSet<>();
		Set<String> doublons = new HashSet<>();
		List<String> listDoubEliminie = new ArrayList<>();
		String req = "";
		for (ParsedQuery requette : requettes) {
			results = processAQuery(requette); // Traitement de la requête, à adapter/réécrire // pour votre programme
			all_results.add(results);
			req = requette.getSourceString().replaceAll("\\s", "");
			// doublons
			/*
			 * if(!doublons.add(req) && !listDoubEliminie.contains(req)) {
			 * nbRequeteDouble++; listDoubEliminie.add(req); }
			 */

		}
		return all_results;
	}

	private static void warmUpSystem(List<ParsedQuery> queries, int warmPercentage) throws IOException {
		if (warmPercentage <= 0 || warmPercentage > 100)
			return;
		int numberOfQueriesToRun = (int) Math.ceil((double) queries.size() * warmPercentage / 100);
		List<ParsedQuery> selectedQueries = queries.stream().limit(numberOfQueriesToRun).collect(Collectors.toList());
		SPARQLParser sparqlParser = new SPARQLParser();
		for (ParsedQuery query : selectedQueries)
			processAQuery(query);
	}

	private static void resultsToCsv(String pathFile, List<ParsedQuery> requettes, List<String> allResults2) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(pathFile, false))) {
			writer.write("\"Request\",\"Results\"");
			int length = requettes.size();
			for (int i = 0; i < length; i++) {
				writer.newLine();
				writer.write(requettes.get(i) + "," + allResults2.get(i));
			}
		} catch (IOException e) {
			System.err.println("Erreur lors de l'écriture dans le fichier CSV : " + e.getMessage());
		}
	}

	private static void statsToCsv(String pathFile, String warmType, int nb_trip, int nb_requettes, long data_read_time,
			long query_read_time, int nbQueriesPerSeconde, long dic_time, long index_time, long eval_time,
			long tot_time, int nb_no_rep, int nb_rep, double pourcentage, double timeJena, String nbQueriesSamePatterns,
			int nbRequeteDouble, String tempsPattern, String ramPattern, double memoireUtilisee, int nbTotalPattern, String NbResponsesPerNbQueriesCSV) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(pathFile, true))) {
			File fichier = new File(pathFile);
			String qr;
			if (useFolder)
				qr = "folder : " + queryFolder;
			else
				qr = queryFile;
			if (fichier.length() != 0) {
				writer.write(dataFile + "," + qr + "," + warmType + "," + nb_trip + "," + nb_requettes + ","
						+ data_read_time + "," + query_read_time + "," + nbQueriesPerSeconde + "," + dic_time + ","
						+ "1 - OPS" + "," + index_time + "," + eval_time + "," + tot_time + "," + nb_no_rep + ","
						+ nb_rep + "," + pourcentage + "," + timeJena + "," + nbQueriesSamePatterns + ","
						+ nbRequeteDouble + "," + tempsPattern + "," + ramPattern + "," + memoireUtilisee + ","
						+ nbTotalPattern+","+NbResponsesPerNbQueriesCSV);
				writer.newLine();

			} else {
				writer.write(
						"\"Data file name\", \"query file name\", \"warm\",\"RDF triplets number\", \"query number\",\"Data reading time(ms)\",\"Query reading time(ms)\" ,\"Nb requete treated/S \",\"Dictionary construction time(ms)\", \"Index number\",\"Index creation time\",\"Workload evaluation time (ms)\",\"Total time(ms)\",\"Nb no response request\",\"Nb response\",\"% response equal to Jena\",\"Jena process time(ms)\",\"NB Queries/NB Patterns\",\"NB duplicates requete\",\"time ((100 requêtes)/ NB pattern)\",\"Ram used/NB Pattern\",\"total Ram used GO\",\"NB total patterns\",\"NB response / Nb queries\"");
				writer.newLine();
				writer.write(dataFile + "," + qr + "," + warmType + "," + nb_trip + "," + nb_requettes + ","
						+ data_read_time + "," + query_read_time + "," + nbQueriesPerSeconde + "," + dic_time + ","
						+ "1 - OPS" + "," + index_time + "," + eval_time + "," + tot_time + "," + nb_no_rep + ","
						+ nb_rep + "," + pourcentage + "," + timeJena + "," + nbQueriesSamePatterns + ","
						+ nbRequeteDouble + "," + tempsPattern + "," + ramPattern + "," + memoireUtilisee + ","
						+ nbTotalPattern+","+NbResponsesPerNbQueriesCSV);
				writer.newLine();
			}
		} catch (IOException e) {
			System.err.println("Erreur lors de la création ou de l'écriture dans le fichier CSV : " + e.getMessage());
		}
	}

	private static int checkCompleteness(Set<ParsedQuery> requettes, Model model){
		int nbEqualReqJena=0;
		for (ParsedQuery requette : requettes) {	
			results = processAQuery(requette);			
			allResults.addAll(results);		
			resultsJena = processJENA(requette.getSourceString(), model);
			if (results.equals(resultsJena)) nbEqualReqJena++;			
		}
		return nbEqualReqJena;	
	}
	public static void main(String[] args) throws Exception {

			long full_time, first_time, memoireStart;

			String warmType = "";
			Runtime runtime = Runtime.getRuntime();
			memoireStart = runtime.totalMemory() - runtime.freeMemory();
			first_time = System.currentTimeMillis();
			int nbEqualReqJena = 0, nb_vide = 0, nb_full = 0, nbTotalPattern = 0;
			Model model = null;
			double pourcentage_jena = 0.0, memoireUtilisee = 0.0;
			long startCheckJena = 0, endCheckJena = 0, startCheckSystem = 0, endCheckSystem = 0, requettes_time,
					start_data, data_time, start_requettes;
			String nbRequetePattern, ramPattern, timeExec100Req, NbResponsesPerNbQueriesCSV;
			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
				case "-queries":
					useFolder = true;
					queryFolder = args[++i];
					break;
				case "-data":
					dataFile = args[++i];
					break;
				case "-output":
					outputDir = args[++i];
					csvFile = outputDir + "/csvfile.csv";
					statscsv = outputDir + "/statscsv.csv";
					break;
				case "-jena":
					useJena = true;
					break;
				case "-warm":
					warm = Integer.parseInt(args[++i]);
					// activer la fonction warm (chauffer le système)
					break;
				case "-shuffle":
					shuffle = true;
				case "-export_query_results":
					exportResultCSV=true;
					break;
				default:
					System.err.println("Mauvais argument");
					// return;
				}
			}

			start_data = System.currentTimeMillis();
			MainRDFHandler mainRdfHandler = parseData();
			data_time = System.currentTimeMillis() - start_data;

			start_requettes = System.currentTimeMillis();

			List<ParsedQuery> requettes = new ArrayList<>();
			if (useFolder) {
				requettes = parseQueriesDirectory(queryFolder);
			} else {
				requettes = parseQueries(queryFile);
			}

			requettes_time = System.currentTimeMillis() - start_requettes;

			if (shuffle)
				Collections.shuffle(requettes);

			if (warm > 0) {
				warmUpSystem(requettes, warm);
				warmType = "warm " + warm + " %";
			} else
				warmType = "cold";

			if (useJena) {
				model = ModelFactory.createDefaultModel();
				model.read(dataFile, "N-TRIPLET");
			}

			List<String> allQueries = requettes.stream().map(s -> s.getSourceString().replaceAll("\\s", ""))
					.collect(Collectors.toList());
			Set<String> reqSansDoublons = new HashSet<>(allQueries);
			nbRequeteDouble = allQueries.size() - reqSansDoublons.size();

			for (ParsedQuery requette : requettes) {
				startCheckSystem = System.currentTimeMillis();
				results = processAQuery(requette);
				if (results.isEmpty())
					nb_vide++;
				else
					nb_full++;

				endCheckSystem += System.currentTimeMillis() - startCheckSystem;
				allResults.addAll(results);
				if (useJena) {
					startCheckJena = System.currentTimeMillis();
					resultsJena = processJENA(requette.getSourceString(), model);
					endCheckJena += (System.currentTimeMillis() - startCheckJena);
				}
				if (results.equals(resultsJena))
					nbEqualReqJena++;
			}
			/*if(exportResultCSV) {
				resultsToCsv( "/qengine-master/output/csvfile.csv ",requettes, allResults);			
				
			}*/
			Map<Integer, Long> execTimePatterns100Requetes = sameNBPatternsMap.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, entry -> {
						long tempsTotalExecution = timeTotPerPatternMap.getOrDefault(entry.getKey(), 0L);
						return (long) (entry.getValue() * (double) tempsTotalExecution / entry.getValue() * 100);
					}));
			Long memoireEnd = runtime.totalMemory() - runtime.freeMemory();
			memoireUtilisee = (double) (memoireEnd - memoireStart) / (1000000000.0);
			pourcentage_jena = (double) nbEqualReqJena / (double) requettes.size();
			full_time = System.currentTimeMillis() - first_time;
			String nbReqSamePattern = "{" + sameNBPatternsMap.entrySet().stream()
					.map(entry -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining("      |   "))
					+ "}";
			nbTotalPattern = sameNBPatternsMap.entrySet().stream().mapToInt(entry -> entry.getKey() * entry.getValue())
					.sum();
			ramPattern = "{" + ramTotPerPatternMap.entrySet().stream()
					.map(entry -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining("      |   "))
					+ "}";
			timeExec100Req = "{" + execTimePatterns100Requetes.entrySet().stream()
					.map(entry -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining("      |   "))
					+ "}";
			NbResponsesPerNbQueriesCSV= "{" + nbResponsePerNbQuery.entrySet().stream()
					.map(entry -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining("      |   "))
					+ "}";
			nbQueriesPerSeconde = (requettes.size() / (int) requettes_time) * 1000;
			statsToCsv(statscsv, warmType, mainRdfHandler.getNb_trip(), requettes.size(), data_time, requettes_time,
					nbQueriesPerSeconde, mainRdfHandler.getDic_Time(), mainRdfHandler.getIndex_Time(), endCheckSystem,
					full_time, nb_vide, nb_full, pourcentage_jena * 100.0, endCheckJena, nbReqSamePattern,
					nbRequeteDouble, timeExec100Req, ramPattern, memoireUtilisee, nbTotalPattern, NbResponsesPerNbQueriesCSV);

		}
	

}
