package qengine.program;

import java.util.HashMap;
import java.util.Map;

public class Dictionnary {
	
	Map<Integer, String> dictionnary;
	Map<String, Integer> dictionnary2;
	static int key=1;

	public Dictionnary() {
		super();
		this.dictionnary = new HashMap<>(); 
		this.dictionnary2=new HashMap<>();
	}

	public Map<Integer, String> getDictionnary() {
		return dictionnary;
	}

	public void setDictionnary(Map<Integer, String> dictionnary) {
		this.dictionnary = dictionnary;
	}
	
	
	
	public Map<String, Integer> getDictionnary2() {
		return dictionnary2;
	}

	public void setDictionnary2(Map<String, Integer> dictionnary2) {
		this.dictionnary2 = dictionnary2;
	}

	public int addElement( String value ) {	
		if(!this.dictionnary.containsValue(value)) {
			this.dictionnary.put(key, value);
			this.dictionnary2.put(value, key) ;	
			this.key ++;	
			return key-1;
		}
		return -1;
	}

	public int getKey(String value) {
		Object val=this.dictionnary2.get(value);
		return val !=null  ? (Integer) val : -1; 
	}

	public String getValue(int key){
		return this.dictionnary.get(key);
	}
	
	public void afficherDictionnaire() {
		Map<Integer, String> dic=this.dictionnary;
		for (Map.Entry<Integer, String> entry : dic.entrySet()) 
	       System.out.println(entry.getKey()+" "+entry.getValue());
	}

}
