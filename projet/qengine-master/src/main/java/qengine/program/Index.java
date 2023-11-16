package qengine.program;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Index {
	
	private ArrayList<ArrayList<Integer>> spo;
	private ArrayList<ArrayList<Integer>> sop;
	private ArrayList<ArrayList<Integer>> pso;
	private ArrayList<ArrayList<Integer>> pos;
	private ArrayList<ArrayList<Integer>> osp; 
	private ArrayList<ArrayList<Integer>> ops; 
	
	public Index() {
		super();
		this.spo=new ArrayList<>();
		this.sop=new ArrayList<>();
		this.pso=new ArrayList<>();
		this.pos=new ArrayList<>();
		this.osp=new ArrayList<>();
		this.ops=new ArrayList<>();
		
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

	public void addIndex(Map<Integer, String> map, String subject, String predicate ,String object ){	
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
		
		this.spo.add( new ArrayList<>(Arrays.asList( indexSub,indexPred ,indexOb )));
		this.sop.add( new ArrayList<>(Arrays.asList( indexSub, indexOb, indexPred )));
		this.pso.add( new ArrayList<>(Arrays.asList( indexPred, indexSub, indexOb )));
		this.pos.add( new ArrayList<>(Arrays.asList( indexPred, indexOb, indexSub )));
		this.osp.add( new ArrayList<>(Arrays.asList( indexOb, indexSub ,indexPred )));
		this.ops.add( new ArrayList<>(Arrays.asList(indexOb ,indexPred ,indexSub )));
	}
	
	public ArrayList<ArrayList<Integer>> trierListe(ArrayList<ArrayList<Integer>> list) {
        list.sort(new Comparator<List<Integer>>() {
            @Override
            public int compare(List<Integer> list1, List<Integer> list2) {
                int size = Math.min(list1.size(), list2.size());

                for (int i = 0; i < size; i++) {
                    int compareResult = list1.get(i).compareTo(list2.get(i));
                    if (compareResult != 0) {
                        return compareResult;
                    }
                }

                return Integer.compare(list1.size(), list2.size());
            }
        });

        return list;
    }
	
}
