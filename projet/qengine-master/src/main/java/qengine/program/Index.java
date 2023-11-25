package qengine.program;

import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashSet;

public class Index {
	
	Map<Integer, Map<Integer, HashSet<Integer>>> spo;
	Map<Integer, Map<Integer, HashSet<Integer>>> sop;
	Map<Integer, Map<Integer, HashSet<Integer>>> pso;
	Map<Integer, Map<Integer, HashSet<Integer>>> pos;
	Map<Integer, Map<Integer, HashSet<Integer>>> osp;
	Map<Integer, Map<Integer, HashSet<Integer>>> ops;
	
	
	/*private HashSet<HashSet<Integer>> spo;
	private HashSet<HashSet<Integer>> sop;
	private HashSet<HashSet<Integer>> pso;
	private HashSet<HashSet<Integer>> pos;
	private HashSet<HashSet<Integer>> osp; 
	private HashSet<HashSet<Integer>> ops; */
	
	public Index() {
		super();
		this.spo=new HashMap<>();
		this.sop=new HashMap<>();
		this.pso=new HashMap<>();
		this.pos=new HashMap<>();
		this.osp=new HashMap<>();
		this.ops=new HashMap<>();
		
	}
	
	public Map<Integer, Map<Integer, HashSet<Integer>>> getSpo() {
		return spo;
	}


	public void HashSetSpo(Map<Integer, Map<Integer, HashSet<Integer>>> spo) {
		this.spo = spo;
	}

	public Map<Integer, Map<Integer, HashSet<Integer>>> getSop() {
		return sop;
	}

	public void HashSetSop(Map<Integer, Map<Integer, HashSet<Integer>>> sop) {
		this.sop = sop;
	}

	public Map<Integer, Map<Integer, HashSet<Integer>>> getPso() {
		return pso;
	}

	public void HashSetPso(Map<Integer, Map<Integer, HashSet<Integer>>> pso) {
		this.pso = pso;
	}

	public Map<Integer, Map<Integer, HashSet<Integer>>> getPos() {
		return pos;
	}



	public void HashSetPos(Map<Integer, Map<Integer, HashSet<Integer>>> pos) {
		this.pos = pos;
	}



	public Map<Integer, Map<Integer, HashSet<Integer>>> getOsp() {
		return osp;
	}



	public void HashSetOsp(Map<Integer, Map<Integer, HashSet<Integer>>> osp) {
		this.osp = osp;
	}



	public Map<Integer, Map<Integer, HashSet<Integer>>> getOps() {
		return ops;
	}



	public void HashSetOps(Map<Integer, Map<Integer, HashSet<Integer>>> ops) {
		this.ops = ops;
	}
	
	public void addIndex(Map<Integer, String> map, String subject, String predicate ,String object ) {
		int indexSub, indexPred, indexOb;
		indexSub=0;
		indexPred=0;
		indexOb=0;
		
		for (Map.Entry<Integer, String> entry : map.entrySet()) {
			 if (entry.getValue().equals(subject)) {
	             indexSub = entry.getKey();
	        } 
	        if (entry.getValue().equals(predicate)) {
	             indexPred = entry.getKey();               
	        }
	        if (entry.getValue().equals(object)) {
	             indexOb  = entry.getKey();
	        }		       	
		}
		
		//spo
		if(spo.containsKey(indexSub)) {
			Map<Integer, HashSet<Integer>> dicPredicate= spo.get(indexSub);
			if(dicPredicate.containsKey(indexPred)) {
				HashSet<Integer> listObject=dicPredicate.get(indexPred);
				if(!listObject.contains(indexOb)) listObject.add(indexOb);	
			}else {
				HashSet<Integer> newListObject= new HashSet<>();
				newListObject.add(indexOb);
				dicPredicate.put(indexPred, newListObject);		
			}	
		}else{
			HashSet<Integer> newListObject= new HashSet<>();
			newListObject.add(indexOb);
			Map<Integer, HashSet<Integer>> newDicPredicate=new HashMap<>();
			newDicPredicate.put(indexPred, newListObject);
			spo.put(indexSub, newDicPredicate);		
		}
		
		
		
	
	
		//POS
		if(pos.containsKey(indexPred)) {
			Map<Integer, HashSet<Integer>> dicObject= pos.get(indexPred);
			if(dicObject.containsKey(indexOb)) {
			HashSet<Integer> listSubject=dicObject.get(indexOb);
			if(!listSubject.contains(indexSub)) listSubject.add(indexSub);
			}else {
			HashSet<Integer> newListSubject= new HashSet<>();
			newListSubject.add(indexSub);
			dicObject.put(indexOb, newListSubject);
			}
		}else{
			HashSet<Integer> newListSubject= new HashSet<>();
			newListSubject.add(indexSub);
			Map<Integer, HashSet<Integer>> newDicObject=new HashMap<>();
			newDicObject.put(indexOb, newListSubject);
			pos.put(indexPred, newDicObject);
		}


		//OPS
		if(ops.containsKey(indexOb)) {
			Map<Integer, HashSet<Integer>> dicPredicate= ops.get(indexOb);
			if(dicPredicate.containsKey(indexPred)) {
			HashSet<Integer> listSubject=dicPredicate.get(indexPred);
			if(!listSubject.contains(indexSub)) listSubject.add(indexSub);
			}else {
			HashSet<Integer> newListSubject= new HashSet<>();
			newListSubject.add(indexSub);
			dicPredicate.put(indexPred, newListSubject);
			}
		}else{
			HashSet<Integer> newListSubject= new HashSet<>();
			newListSubject.add(indexSub);
			Map<Integer, HashSet<Integer>> newDicPredicate=new HashMap<>();
			newDicPredicate.put(indexPred, newListSubject);
			ops.put(indexOb, newDicPredicate);
		}


	}


	public  void displayMap(Map<Integer, Map<Integer, HashSet<Integer>>> dic, String nom) {
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
    }
		
		
		
}


	
	

