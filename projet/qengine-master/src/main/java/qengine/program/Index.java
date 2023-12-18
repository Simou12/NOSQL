package qengine.program;

import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.rdf4j.model.Statement;

import java.util.HashSet;

public class Index {
	
	Map<Integer, Map<Integer, HashSet<Integer>>> pos;
	Map<Integer, Map<Integer, HashSet<Integer>>> ops;
	Map<Integer, Map<Integer, HashSet<Integer>>> spo;
	Map<Integer, Map<Integer, HashSet<Integer>>> pso;	
	Map<Integer, Map<Integer, HashSet<Integer>>> osp;
	Map<Integer, Map<Integer, HashSet<Integer>>> sop;
	
	public Index() {
		super();
		this.pos=new HashMap<>();
		this.ops=new HashMap<>();
		this.spo=new HashMap<>();
		this.pso=new HashMap<>();
		this.osp=new HashMap<>();
		this.sop=new HashMap<>();
		
	}
	

	public Map<Integer, Map<Integer, HashSet<Integer>>> getPos() {
		return pos;
	}



	public void setPos(Map<Integer, Map<Integer, HashSet<Integer>>> pos) {
		this.pos = pos;
	}



	public Map<Integer, Map<Integer, HashSet<Integer>>> getOps() {
		return ops;
	}



	public void setOps(Map<Integer, Map<Integer, HashSet<Integer>>> ops) {
		this.ops = ops;
	}
	
	public void addValToIndex(Map<Integer, Map<Integer, HashSet<Integer>>> index, Integer key1, Integer key2, Integer value) {
		 index.computeIfAbsent(key1, k -> new HashMap<>())
        .computeIfAbsent(key2, k -> new HashSet<>())
        .add(value);
	}
	
	public void addIndex(Statement st, Dictionnary dictionnary) {
		int indexSub=dictionnary.getKey(st.getSubject().toString());
		int indexPred=dictionnary.getKey(st.getPredicate()+"");
		int indexOb=dictionnary.getKey(st.getObject().toString());
		
		addValToIndex(ops, indexOb, indexPred, indexSub);
		addValToIndex(pos, indexPred, indexOb, indexSub);
		addValToIndex(spo, indexSub, indexPred, indexOb);
		addValToIndex(pso, indexPred, indexSub, indexOb);
		addValToIndex(sop, indexSub, indexOb, indexPred);
		addValToIndex(osp, indexOb, indexSub, indexPred);		
	}


	/*public  void displayMap(Map<Integer, Map<Integer, HashSet<Integer>>> dic, String nom) {
		System.out.println(nom.toUpperCase()+"\n");
        for (Map.Entry<Integer, Map<Integer, HashSet<Integer>>> entry : dic.entrySet()) {
            Integer key1 = entry.getKey();
            Map<Integer, HashSet<Integer>> innerMap = entry.getValue();

            System.out.println("Clé externe: " + key1);

            for (Map.Entry<Integer, HashSet<Integer>> innerEntry : innerMap.entrySet()) {
                Integer key2 = innerEntry.getKey();
                HashSet<Integer> set = innerEntry.getValue();
                System.out.println("   Clé interne: " + key2 + ", Ensemble de valeurs: " + set);
            }
        }
    }*/
		
		
		
}


	
	

