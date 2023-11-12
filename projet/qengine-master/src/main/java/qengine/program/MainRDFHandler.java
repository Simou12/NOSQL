package qengine.program;

import java.util.ArrayList;
import java.util.Arrays;
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
	
	Map<Integer, String> dictionnary=new HashMap<>();
	private ArrayList<ArrayList<Integer>> spo=new ArrayList<>();
	private ArrayList<ArrayList<Integer>> sop=new ArrayList<>(); 
	private ArrayList<ArrayList<Integer>> pso=new ArrayList<>(); 
	private ArrayList<ArrayList<Integer>> pos=new ArrayList<>();
	private ArrayList<ArrayList<Integer>> osp=new ArrayList<>(); 
	private ArrayList<ArrayList<Integer>> ops=new ArrayList<>(); 
	String subject,predicate, object;
	int indexSub=0;
	int indexOb=0;
	int indexPred=0;
	int index = 1;
	boolean bSub, bObj, bPred;
	
	public Map<Integer, String> getDictionnary() {
		return dictionnary;
	}
	public void setDictionnary(Map<Integer, String> dictionnary) {
		this.dictionnary = dictionnary;
	}
	public ArrayList<ArrayList<Integer>> getSpo() {
		return spo;
	}
	public void setSpo(ArrayList<ArrayList<Integer>> spo) {
		this.spo = spo;
	}
	public ArrayList<ArrayList<Integer>> getSop() {
		return sop;
	}
	public void setSop(ArrayList<ArrayList<Integer>> sop) {
		this.sop = sop;
	}
	public ArrayList<ArrayList<Integer>> getPso() {
		return pso;
	}
	public void setPso(ArrayList<ArrayList<Integer>> pso) {
		this.pso = pso;
	}
	public ArrayList<ArrayList<Integer>> getPos() {
		return pos;
	}
	public void setPos(ArrayList<ArrayList<Integer>> pos) {
		this.pos = pos;
	}
	public ArrayList<ArrayList<Integer>> getOsp() {
		return osp;
	}
	public void setOsp(ArrayList<ArrayList<Integer>> osp) {
		this.osp = osp;
	}
	public ArrayList<ArrayList<Integer>> getOps() {
		return ops;
	}
	public void setOps(ArrayList<ArrayList<Integer>> ops) {
		this.ops = ops;
	}
	@SuppressWarnings("deprecation")
	@Override
	
	public void handleStatement(Statement st) {
		subject=st.getSubject().toString();
		predicate=st.getPredicate()+"";
		object=st.getObject().toString();
		bSub=false;
		bObj=false;
		bPred=false;
	
		if(!dictionnary.containsValue(subject)) {
			dictionnary.put(index, subject);
			index ++;	
			indexSub=index;
			bSub=true;
		}
		
		if(!dictionnary.containsValue(predicate)) {
			dictionnary.put(index, predicate);
			index ++;	
			bPred=true;
			indexPred=index;
			
		}
		if(!dictionnary.containsValue(object)) {
			dictionnary.put(index, object);
			index ++;
			bObj=true;
			indexOb =index;
		}
		
		if ( !bSub || !bPred || !bObj ) {	
			for (Map.Entry<Integer, String> entry : dictionnary.entrySet()) {
		        if ( !bSub && entry.getValue().equals(subject)) {
		             indexSub = entry.getKey();
		        } 
		        if (!bPred && entry.getValue().equals(predicate)) {
		             indexPred = entry.getKey();               
		        }
		        if (!bObj && entry.getValue().equals(object)) {
		             indexOb  = entry.getKey();
		        }		      
		        
		    }
		}
		
		spo.add( new ArrayList<>(Arrays.asList( indexSub,indexPred ,indexOb )));
		sop.add( new ArrayList<>(Arrays.asList( indexSub, indexOb, indexPred )));
		pso.add( new ArrayList<>(Arrays.asList( indexPred, indexSub, indexOb )));
		pos.add( new ArrayList<>(Arrays.asList( indexPred, indexOb, indexSub )));
		osp.add( new ArrayList<>(Arrays.asList( indexOb, indexSub ,indexPred )));
		ops.add( new ArrayList<>(Arrays.asList(indexOb ,indexPred ,indexSub )));	
		
	}
	
	
	
}