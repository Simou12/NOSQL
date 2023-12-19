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

	long dic_time=0;
	long index_time=0;
	int nb_trip =0;
	Dictionnary dictionnary;
	Index index;
	
	public MainRDFHandler() {
		this.dictionnary= new Dictionnary();
		this.index=new Index();
	}
	
	
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

	public long getDic_Time() {
		return  dic_time;
	}

	public long getIndex_Time() {
		return  index_time;
	}

	public int getNb_trip(){
		return nb_trip;
	}

	
	@Override
	
	public void handleStatement(Statement st) {	
		nb_trip++;
		
		long start_dic = System.currentTimeMillis();
		dictionnary.constructDic(st);
		long end_dic = System.currentTimeMillis();
		index.addIndex(st, this.dictionnary);	
		long end_index = System.currentTimeMillis();
		index_time = index_time + end_index- end_dic;
		dic_time = dic_time + end_dic - start_dic;
	}


	
	
	
	
}