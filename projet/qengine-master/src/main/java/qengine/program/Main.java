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

import org.apache.commons.lang3.ObjectUtils.Null;
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
	public static HashSet<Integer> processAQuery(ParsedQuery query,MainRDFHandler mainRdfHandler) {
		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
		Dictionnary dictionnary = mainRdfHandler.getDictionnary(); 
		Index index = mainRdfHandler.getIndex();
		java.util.Map<Integer, java.util.Map<Integer, HashSet<Integer>>> ops = index.getOps();
		java.util.Map<Integer, java.util.Map<Integer, HashSet<Integer>>> osp = index.getPos();


		 

		//System.out.println("first pattern : " + patterns.get(0));

		//System.out.println("object of the first pattern : " + patterns.get(0).getObjectVar().getValue());

		//System.out.println("predicate of the first pattern : " + patterns.get(0).getPredicateVar().getValue());

		//System.out.println("variables to project : ");
		
		
		
		HashSet<Integer> result ;

		// first pattern 

		int indexOb = dictionnary.getKey(patterns.get(0).getObjectVar().getValue().toString());


		int indexPred = dictionnary.getKey(patterns.get(0).getPredicateVar().getValue().toString());
		//System.out.println("indexOb : " + indexOb + "     indexPred : "+ indexPred);
		if(ops.containsKey(indexOb)) {
			java.util.Map<Integer, HashSet<Integer>> dicPredicate= ops.get(indexOb);
			if(dicPredicate.containsKey(indexPred)) {
				result=dicPredicate.get(indexPred);
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
		
		for (int sub : result){
			System.out.println(dictionnary.getValue(sub));
		}
		System.out.println();
		return result ;
		


		/*// Utilisation d'une classe anonyme
		query.getTupleExpr().visit(new AbstractQueryModelVisitor<RuntimeException>() {

			public void meet(Projection projection) {
				System.out.println(projection.getProjectionElemList().getElements());
			}
		});*/
	}




	/**
	 * Entrée du programme
	 */
	public static void main(String[] args) throws Exception {
		MainRDFHandler mainRdfHandler = parseData();
		System.out.println("hello");
		parseQueries(mainRdfHandler);
	}

	// ========================================================================

	/**
	 * Traite chaque requête lue dans {@link #queryFile} avec {@link #processAQuery(ParsedQuery)}.
	 */
	private static void parseQueries(MainRDFHandler mainRdfHandler) throws FileNotFoundException, IOException {
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
			int i = 0;
			int j = 0;
			
			while (lineIterator.hasNext())
			
			/*
			 * On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'
			 * On considère alors que c'est la fin d'une requête
			 */
			{
				j++;
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					
					i++;
					ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);

					System.out.println();
					System.out.println("querry nb : " + i + " with " + j + " patterns"  );
					processAQuery(query,mainRdfHandler); // Traitement de la requête, à adapter/réécrire pour votre programme
					System.out.println();
					j=0;
			



					queryString.setLength(0); // Reset le buffer de la requête en chaine vide
				}
			}
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
