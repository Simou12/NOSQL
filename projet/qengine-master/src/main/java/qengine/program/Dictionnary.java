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
	
	public int addElement( String value ) {	
		if(!this.dictionnary.containsValue(value)) {
			dictionnary.put(key, value);
			this.key ++;	
			return key-1;
		}
		return -1;
	}

	public int getKey(String value) {
		for (Map.Entry<Integer, String> entry : dictionnary.entrySet()) {
			//System.out.println("looking for : " + value + "   found : " +entry.getValue());
			if (entry.getValue().toString().equals(value.toString())) {
				//System.out.println("           F O U N D   ");
                return entry.getKey();
            }
		}
		return -1;
	}

	public String getValue(int key){
		return dictionnary.get(key);
	}
	
	public void afficherDictionnaire() {
		Map<Integer, String> dic=this.dictionnary;
		for (Map.Entry<Integer, String> entry : dic.entrySet()) 
	       System.out.println(entry.getKey()+" "+entry.getValue());
	}

}
