package qengine.program;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

import com.github.andrewoma.dexx.collection.HashMap;
import com.github.andrewoma.dexx.collection.Map;

import riotcmd.infer;

/**
 * Programme simple lisant un fichier de requête et un fichier de données.
 * 
 * <p>
 * Les entrées sont données ici de manière statique,
 * à vous de programmer les entrées par passage d'arguments en ligne de commande comme demandé dans l'énoncé.
 * </p>
 * 
 * <p>
 * Le présent programme se contente de vous montrer la voie pour lire les triples et requêtes
 * depuis les fichiers ; ce sera à vous d'adapter/réécrire le code pour finalement utiliser les requêtes et interroger les données.
 * On ne s'attend pas forcémment à ce que vous gardiez la même structure de code, vous pouvez tout réécrire.
 * </p>
 * 
 * @author Olivier Rodriguez <olivier.rodriguez1@umontpellier.fr>
 */
final class Main {
	static final String baseURI = null;

	/**
	 * Votre répertoire de travail où vont se trouver les fichiers à lire
	 */
	static final String workingDir = "data/";

	/**
	 * Fichier contenant les requêtes sparql
	 */
	static String queryFile = workingDir + "sample_query.queryset";
											//STAR_ALL_workload
	/**
	 * Fichier contenant des données rdf
	 */
	static String dataFile = workingDir + "sample_data.nt";

	static HashSet<String> queryFiles;
	static String queryFolder;
	static boolean useFolder = false; 

	static String outputDir = "output/";

	static boolean useJena = false;
	static boolean shuffle = false;
	static int warm = 0;

	static String csvFile = outputDir+"csvfile.csv";

	static String statscsv = outputDir+"statscsv.csv";

	
	
	
	
	/**
	 * Méthode utilisée ici lors du parsing de requête sparql pour agir sur l'objet obtenu.
	 */
	public static HashSet<String> processAQuery(ParsedQuery query,MainRDFHandler mainRdfHandler) {
		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
		Dictionnary dictionnary = mainRdfHandler.getDictionnary(); 
		Index index = mainRdfHandler.getIndex();
		java.util.Map<Integer, java.util.Map<Integer, HashSet<Integer>>> ops = index.getOps();
		java.util.Map<Integer, java.util.Map<Integer, HashSet<Integer>>> pos = index.getPos();
		HashSet<Integer> result ;

		// first pattern 

		int indexOb = dictionnary.getKey(patterns.get(0).getObjectVar().getValue().toString());
		int indexPred = dictionnary.getKey(patterns.get(0).getPredicateVar().getValue().toString());
		

		//System.out.println("indexOb : " + indexOb + "     indexPred : "+ indexPred);
		if(ops.containsKey(indexOb)) {
			java.util.Map<Integer, HashSet<Integer>> dicPredicate= ops.get(indexOb);
			if(dicPredicate.containsKey(indexPred)) {
				result=(HashSet)dicPredicate.get(indexPred).clone();
			}else {
				return null;
			}
		}else {
			return null;
		}
		patterns.remove(0);
		

		// all other patterns

		for (StatementPattern pattern : patterns){
			indexOb = dictionnary.getKey(pattern.getObjectVar().getValue().toString());
			indexPred = dictionnary.getKey(pattern.getPredicateVar().getValue().toString());
			//System.out.println("indexOb : " + indexOb + "     indexPred : "+ indexPred);
			if(ops.containsKey(indexOb)) {
				java.util.Map<Integer, HashSet<Integer>> dicPredicate= ops.get(indexOb);
				if(dicPredicate.containsKey(indexPred)) {
					result.retainAll(dicPredicate.get(indexPred));

				}else {
					return null;
				}
			}else {
				return null;
			}
		}
		HashSet<String> text_result = new HashSet<String>();
		for (int sub : result){
			String real_sub = dictionnary.getValue(sub);
			//System.out.println(real_sub);
			text_result.add(real_sub);
		}
		return text_result ;

		/*// Utilisation d'une classe anonyme
		query.getTupleExpr().visit(new AbstractQueryModelVisitor<RuntimeException>() {

			public void meet(Projection projection) {
				System.out.println(projection.getProjectionElemList().getElements());
			}
		});*/

	}


	public static HashSet<String> processJENA(String sparqlQueryString, Model model) {
        // Create a SPARQL query
        Query query = QueryFactory.create(sparqlQueryString);

        // Create a QueryExecution object to execute the query on the given model
        try (QueryExecution queryExecution = QueryExecutionFactory.create(query, model)) {
            // Execute the query and obtain the result set
            ResultSet resultSet = queryExecution.execSelect();

            // Process and print the results
			HashSet<String> results = new  HashSet<String>();

            while (resultSet.hasNext()) {
                QuerySolution solution = resultSet.nextSolution();
                // Example: Print the value of the variable "?s"

				results.add(solution.get("?v0").toString());

                //System.out.println("Value of ?s: " + solution.get("?v0"));
                // You can access other variables in a similar way.
            }
		return results;
        }
    }


	/**
	 * Entrée du programme
	 */
	public static void main(String[] args) throws Exception {
		long first_time = System.currentTimeMillis();

		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "-queries":
				useFolder = true;
				queryFolder =args[++i];
				queryFiles = new HashSet(Stream.of(new File(queryFolder ).listFiles()).filter(file -> !file.isDirectory()).map(File::getName).collect(Collectors.toSet()));
				System.out.println(queryFolder);
				// Gérer le chemin des requêtes
				break;
			case "-data":
				dataFile = args[++i];
				// Gérer le chemin des données
				break;
			case "-output":
				outputDir = args[++i];
				csvFile = outputDir +"csvfile.csv";
				statscsv = outputDir+"statscsv.csv";
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
				// activer la fonction shuffle
				break;
			default:
				
				System.err.println("Mauvais argument");
				//return;
			}
		}
		

		System.out.println("loading queries");

		long start_requettes = System.currentTimeMillis();

		List<String> requettes = new ArrayList<String>();
		if (useFolder) {
			for (String file : queryFiles){
				requettes.addAll(parseQueries(queryFolder+"/"+file));
			}

		}
		else{
			requettes = parseQueries(queryFile);
		}

		long requettes_time = System.currentTimeMillis() - start_requettes;


		if(shuffle){
			System.out.println("shuffling");
			Collections.shuffle(requettes);
		}

		System.out.println("loading data into this system");

		long start_data = System.currentTimeMillis();
		MainRDFHandler mainRdfHandler = parseData();
		long data_time = System.currentTimeMillis() - start_data;

		if(warm>0){
			System.out.println("	warming up this system");
			warmUpSystem(mainRdfHandler, requettes, warm);
		}


		System.out.println("	checking queries using this system");
		long start_check = System.currentTimeMillis();
		List<HashSet<String>> results = processQueries(mainRdfHandler, requettes);
		long check_time = System.currentTimeMillis()-start_check;

		int pourcentage_jena = -1;
		if (useJena){
			Model model = ModelFactory.createDefaultModel();
			System.out.println("Jena :");
			System.out.println("	loading data into Jena");
			model.read(dataFile,"N-TRIPLET");

			if(warm>0){
				System.out.println("	warming up Jena");
				warmUpJena(model, requettes, warm);
			}

			System.out.println("	checking queries using Jena");
			List<HashSet<String>> results_jena = parseQueriesJena(model,requettes);

			int len = results.size();
			Boolean complet = true;
			List<Integer> faute = new ArrayList<>();

			System.out.println("	checked " +  len + " queries");

			for (int i=0 ; i<len ; i++){
				//System.out.println(i);
				if (!(results.get(i) == null) && !(results_jena.get(i) == null)){
								
					if (!results.get(i).equals(results_jena.get(i))){
						complet = false ;
						faute.add(i);
						//System.out.println("M E R D E");
					}else{
						//System.out.println("H A M D O U L E H");
					}		
				}
			}
			pourcentage_jena = 100 - faute.size()/len*100;
			if (complet) {
				System.out.println("	the system is complete");
			}
			else{
				System.out.println("	the system is caught lacking :");


				for (int i:faute){
					System.out.println("            case : "+ i);
					System.out.println();
					System.out.println(requettes.get(i));
					System.out.println();
					System.out.println(" 			our : ");
					for (String value:results.get(i)){
						System.out.println(value);
					}
					System.out.println(" 			JENA : ");
					for (String value:results_jena.get(i)){
						System.out.println(value);
					}
				}
			}


		}


		long full_time = System.currentTimeMillis()-first_time; 

		System.out.println("printing to " + csvFile);
		resultsToCsv(csvFile, requettes, results);

		int nb_vide = 0;
		int nb_full =0;
		int len = results.size();
		for (int i=0 ; i<len ; i++){if (results.get(i) == null){ nb_vide++;}else{nb_full++;}}

		statsToCsv(statscsv,len,data_time,requettes_time,mainRdfHandler.getDic_Time(),mainRdfHandler.getIndex_Time(),check_time,full_time,nb_vide,nb_full,pourcentage_jena);
		//(String pathFile, int nb_requettes, int data_read_time,int query_read_time,int dic_time,int index_time, int eval_time, int tot_time,int nb_no_rep,int nb_rep)
		


	}



	// ========================================================================

	/**
	 * Traite chaque requête lue dans {@link #queryFile} avec {@link #processAQuery(ParsedQuery)}.
	 */
	private static List<String> parseQueries(String filepath) throws FileNotFoundException, IOException {
		/**
		 * Try-with-resources
		 * 
		 * @see <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">Try-with-resources</a>
		 */
		/*
		 * On utilise un stream pour lire les lignes une par une, sans avoir à toutes les stocker
		 * entièrement dans une collection.
		 */
		try (Stream<String> lineStream = Files.lines(Paths.get(filepath))) {
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();
			List<String> requettes = new ArrayList<>();
			
			while (lineIterator.hasNext())
			
			/*
			 * On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'
			 * On considère alors que c'est la fin d'une requête
			 */
			{
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					requettes.add(queryString.toString());
					queryString.setLength(0); // Reset le buffer de la requête en chaine vide
				}
			}

			return requettes;
		}
	}

	private static List<HashSet<String>> processQueries(MainRDFHandler mainRdfHandler,List<String> requettes) throws FileNotFoundException, IOException {

			SPARQLParser sparqlParser = new SPARQLParser();
			List<HashSet<String>> all_results = new ArrayList<HashSet<String>>();
			
			
			for(String requette:requettes){
				ParsedQuery query = sparqlParser.parseQuery(requette, baseURI);
				HashSet<String> results = processAQuery(query,mainRdfHandler); // Traitement de la requête, à adapter/réécrire pour votre programme
				all_results.add(results);
			}

			return all_results;
	
	}

	
	private static List<HashSet<String>> parseQueriesJena(Model model,List<String> requettes) throws FileNotFoundException, IOException {
			List<HashSet<String>> all_results = new ArrayList<HashSet<String>>();

			for(String requette:requettes){
				all_results.add(processJENA(requette, model));
			}
			return all_results;
		
	}
	
	/**
	 * Traite chaque triple lu dans {@link #dataFile} avec {@link MainRDFHandler}.
	 */
	private static MainRDFHandler parseData() throws FileNotFoundException, IOException {

		try (Reader dataReader = new FileReader(dataFile)) {

			// On va parser des données au format ntriples
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
			MainRDFHandler mainRdfHandler=new MainRDFHandler();
			// On utilise notre implémentation de handler
			rdfParser.setRDFHandler(mainRdfHandler);
			// Parsing et traitement de chaque triple par le handler
			rdfParser.parse(dataReader, baseURI);	
			java.util.Map<Integer, String> dictionnary= mainRdfHandler.getDictionnary().getDictionnary();
			/*for (Entry<Integer, String> entry : ( dictionnary).entrySet()) {
				System.out.println(entry.getKey()+" ; "+entry.getValue());
			}*/
			Index index=mainRdfHandler.getIndex();
			
		 java.util.Map<Integer, java.util.Map<Integer, HashSet<Integer>>> pos=index.getPos();
		 java.util.Map<Integer, java.util.Map<Integer, HashSet<Integer>>> ops=index.getOps(); 

		 //index.displayMap(pos,"");
		  
		/* System.out.println(" Le dictionnaire : \n");
		 mainRdfHandler.getDictionnary().afficherDictionnaire();
		 
		 System.out.println(" #################Les index ################# \n");
		
		 index.displayMap(spo,"spo");
		 index.displayMap(sop,"sop");
		 index.displayMap(pso,"pso");
		 index.displayMap(pos,"pos");
		 index.displayMap(osp,"osp");
		 index.displayMap(ops,"ops");*/
		 
		 return (mainRdfHandler);
		} 	
	}
	
	private static void warmUpSystem(MainRDFHandler mainRdfHandler,List<String> queries,int warmPercentage) throws IOException {
        if (warmPercentage <= 0 || warmPercentage > 100) {
            System.out.println("		Invalide pourcentage warm-up, warm-up ignoré.");
            return;
        }
        int numberOfQueriesToRun = (int) Math.ceil((double) queries.size() * warmPercentage / 100);
        System.out.println("		warming with " + numberOfQueriesToRun+ " queries");
        // Collections.shuffle(queries); // Mélanger la liste des requêtes
        List<String> selectedQueries = queries.stream().limit(numberOfQueriesToRun).collect(Collectors.toList());
		SPARQLParser sparqlParser = new SPARQLParser();

        for (String query : selectedQueries) {
            // System.out.println("je warm avec la requete: " + query);
			// Traitement de la requête pour le warm-up
			
			ParsedQuery p_query = sparqlParser.parseQuery(query, baseURI);
			processAQuery(p_query,mainRdfHandler);
        }
        System.out.println("		Warm up terminé");
    }
	
	private static void warmUpJena(Model model,List<String> queries,int warmPercentage) throws IOException {
        if (warmPercentage <= 0 || warmPercentage > 100) {
            System.out.println("		Invalide pourcentage warm-up, warm-up ignoré.");
            return;
        }
        int numberOfQueriesToRun = (int) Math.ceil((double) queries.size() * warmPercentage / 100);
        System.out.println("		warming with " + numberOfQueriesToRun+ " queries");
        // Collections.shuffle(queries); // Mélanger la liste des requêtes
        List<String> selectedQueries = queries.stream().limit(numberOfQueriesToRun).collect(Collectors.toList());
		SPARQLParser sparqlParser = new SPARQLParser();
		
        for (String query : selectedQueries) {
			processJENA(query, model);
        }
        System.out.println("		Warm up terminé");
    }
	
	private static void resultsToCsv(String pathFile, List<String> requettes,List<HashSet<String>> results) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(pathFile,false))) {
			writer.write("\"Request\",\"Results\"");

			int length = requettes.size();
			for (int i =0;i<length;i++){
				writer.newLine();
				writer.write(requettes.get(i) + "," + results.get(i));
			}
		} catch (IOException e) {
			System.err.println("Erreur lors de l'écriture dans le fichier CSV : " + e.getMessage());
		}
	}

	private static void statsToCsv(String pathFile,int nb_requettes, long data_read_time,long query_read_time,long dic_time,long index_time, long eval_time, long tot_time,int nb_no_rep,int nb_rep,int pourcentage) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(pathFile,true))) {
			File fichier = new File(pathFile);
			String qr;
			if (useFolder) {qr = "folder : " + queryFolder;} else{qr=queryFile;}
			
			if (fichier.length() != 0){
				writer.write(dataFile +","+ qr +","+ "RDF triplets number" +","+ nb_requettes +","+ data_read_time +","+ query_read_time +","+ dic_time +","+ "1 - OPS" +","+ index_time +","+ eval_time +","+ tot_time +","+ nb_no_rep +","+ nb_rep +","+ pourcentage);
				writer.newLine();
				System.out.println("exist");
			}
			else{
				writer.write("\"Data file name\", \"query file name\", \"RDF triplets number\", \"query number\",\"Data reading time(ms)\",\"Query reading time(ms)\" ,\"Dictionary construction time(ms)\", \"Index number\",\"Index creation time\",\"Workload evaluation time (ms)\",\"Total time(ms)\",\"Nb no response request\",\"Nb response\",\"% response equal to Jena\"");	
				writer.newLine();
				writer.write(dataFile +","+ qr +","+ "RDF triplets number" +","+ nb_requettes +","+ data_read_time +","+ query_read_time +","+ dic_time +","+ "1 - OPS" +","+ index_time +","+ eval_time +","+ tot_time +","+ nb_no_rep +","+ nb_rep +","+ "100%");
				writer.newLine();
				System.out.println("no exist");
			}

		} catch (IOException e) {
			System.err.println("Erreur lors de la création ou de l'écriture dans le fichier CSV : " + e.getMessage());
		}
	}

        
}
