package qengine.program;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Map;
import java.util.HashMap;

import org.apache.jena.sparql.algebra.op.OpReduced;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

/**
 * Le RDFHandler intervient lors du parsing de données et permet d'appliquer un traitement pour chaque élément lu par le parseur.
 * 
 * <p>
 * Ce qui servira surtout dans le programme est la méthode {@link #handleStatement(Statement)} qui va permettre de traiter chaque triple lu.
 * </p>
 * <p>
 * À adapter/réécrire selon vos traitements.
 * </p>
 */

public final class MainRDFHandler extends AbstractRDFHandler {
	
	String subject,predicate, object;	
	
	Dictionnary dictionnary=new Dictionnary();
	
	Index index=new Index();
	
	public Dictionnary getDictionnary() {
		return dictionnary;
	}

	public void setDictionnary(Dictionnary dictionnary) {
		this.dictionnary = dictionnary;
	}

	public Index getIndex() {
		return index;
	}


	public void setIndex(Index index) {
		this.index = index;
	}


	@SuppressWarnings("deprecation")
	@Override
	
	public void handleStatement(Statement st) {
		
	
		subject=st.getSubject().toString();
		predicate=st.getPredicate()+"";
		object=st.getObject().toString();
		//System.out.println(subject+" "+predicate+" "+object);
		dictionnary.addElement(subject);
		dictionnary.addElement(predicate);
		dictionnary.addElement(object);
		index.addIndex(dictionnary.getDictionnary(), subject, predicate, object);	
	}
	
	
	
	
}