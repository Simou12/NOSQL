package qengine.program;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
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
	static final String queryFile = workingDir + "STAR_ALL_workload.queryset";

	/**
	 * Fichier contenant des données rdf
	 */
	static final String dataFile = workingDir + "100K.nt";
	
	
	
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
		System.out.println("hello");
		System.out.println("loading data into this system");
		MainRDFHandler mainRdfHandler = parseData();
		System.out.println("loading queries and checking them using this system");
		Object[] resultzzz = parseQueries(mainRdfHandler);
		List<HashSet<String>> results = (List<HashSet<String>>)resultzzz[0];
		List<String> requettes = (List<String>)resultzzz[1];

		Model model = ModelFactory.createDefaultModel();
		System.out.println("loading data into Jena");
		model.read(dataFile,"N-TRIPLET");
		System.out.println("checking queries using Jena");
		List<HashSet<String>> results_jena = parseQueriesJena(model);

		int len = results.size();
		Boolean complet = true;
		List<Integer> kaka = new ArrayList<>();

		System.out.println("checked " +  len + " queries");

		for (int i=0 ; i<len ; i++){
			//System.out.println(i);
			if (!(results.get(i) == null) && !(results_jena.get(i) == null)){
							
				if (!results.get(i).equals(results_jena.get(i))){
					complet = false ;
					kaka.add(i);
					//System.out.println("M E R D E");
				}else{
					//System.out.println("H A M D O U L E H");
				}		
			}
		}
		
		if (complet) {
			System.out.println("H A M D O U L E H");
		}
		else{
			System.out.println("M E R D E");


			for (int i:kaka){
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

			if (!mainRdfHandler.index.ops.containsKey(1935)){System.out.println("WHAT WHY WHERE");}
		}



	}



	// ========================================================================

	/**
	 * Traite chaque requête lue dans {@link #queryFile} avec {@link #processAQuery(ParsedQuery)}.
	 */
	private static Object[] parseQueries(MainRDFHandler mainRdfHandler) throws FileNotFoundException, IOException {
		/**
		 * Try-with-resources
		 * 
		 * @see <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">Try-with-resources</a>
		 */
		/*
		 * On utilise un stream pour lire les lignes une par une, sans avoir à toutes les stocker
		 * entièrement dans une collection.
		 */
		try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			SPARQLParser sparqlParser = new SPARQLParser();
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();
			List<String> requettes = new ArrayList<>();
			List<HashSet<String>> all_results = new ArrayList<HashSet<String>>();
			
			while (lineIterator.hasNext())
			
			/*
			 * On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'
			 * On considère alors que c'est la fin d'une requête
			 */
			{
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);
					
					requettes.add(queryString.toString());
					HashSet<String> results = processAQuery(query,mainRdfHandler); // Traitement de la requête, à adapter/réécrire pour votre programme
					all_results.add(results);

					queryString.setLength(0); // Reset le buffer de la requête en chaine vide
				}
			}
			Object[] resultzzz = new Object[2];
			resultzzz[0] = all_results;
			resultzzz[1]=requettes;
			return resultzzz;
		}
	}

	
	private static List<HashSet<String>> parseQueriesJena(Model model) throws FileNotFoundException, IOException {
		
		try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();
			List<HashSet<String>> all_results = new ArrayList<HashSet<String>>();
			
			while (lineIterator.hasNext())
			
			/*
			 * On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'
			 * On considère alors que c'est la fin d'une requête
			 */
			{
				String line = lineIterator.next();
				queryString.append(line);
				String query_str = queryString.toString();

				if (line.trim().endsWith("}")) {

					all_results.add(processJENA(query_str, model));
					queryString.setLength(0); // Reset le buffer de la requête en chaine vide
				}
			}
			return all_results;
		}
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
	
        
}
