package qengine.program;

import java.util.HashMap;
import java.util.Map;

public class Dictionnary {
	
	Map<Integer, String> dictionnary;
	int key=1;

	public Dictionnary() {
		super();
		this.dictionnary = new HashMap<>(); ;
	}

	public Map<Integer, String> getDictionnary() {
		return dictionnary;
	}

	public void setDictionnary(Map<Integer, String> dictionnary) {
		this.dictionnary = dictionnary;
	}
	
	public boolean addElement( String value ) {	
		if(!this.dictionnary.containsValue(value)) {
			dictionnary.put(key, value);
			this.key ++;	
			return true;
		}
		return false;
	}
	
	public void afficherDictionnaire() {
		Map<Integer, String> dic=this.dictionnary;
		for (Map.Entry<Integer, String> entry : dic.entrySet()) 
	       System.out.println(entry.getKey()+" "+entry.getValue());
	}

}
