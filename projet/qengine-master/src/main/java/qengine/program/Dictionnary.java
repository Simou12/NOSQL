package qengine.program;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.Statement;

public class Dictionnary {

	Map<Integer, String> dictionnary;
	Map<String, Integer> dictionnary2;
	static int key = 1;

	public Dictionnary() {
		this.dictionnary = new HashMap<>();
		this.dictionnary2 = new HashMap<>();
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

	public void addElement(String value) {
		if (!this.dictionnary2.containsKey(value)) {
			this.dictionnary.put(key, value);
			this.dictionnary2.put(value, key);
			this.key++;
		}
	}

	public void constructDic(Statement st) {
		addElement(st.getSubject().toString());
		addElement(st.getObject().toString());
		addElement(st.getPredicate() + "");
	}

	public int getKey(String value) {
		Object val = this.dictionnary2.get(value);
		return val != null ? (Integer) val : -1;
	}

	public String getValue(int key) {
		return this.dictionnary.get(key);
	}

	public void afficherDictionnaire() {
		Map<Integer, String> dic = this.dictionnary;
		for (Map.Entry<Integer, String> entry : dic.entrySet())
			System.out.println(entry.getKey() + " " + entry.getValue());
	}

}
/*
import java.util.HashMap;
import java.util.Map;
import org.eclipse.rdf4j.model.Statement;

public class Dictionnary {

    private final Map<Integer, String> dictionary;
    private final Map<String, Integer> dictionary2;
    private int key;

    public Dictionnary() {
        this.dictionary = new HashMap<>();
        this.dictionary2 = new HashMap<>();
        this.key = 1;
    }

    public Map<Integer, String> getDictionnary() {
        return dictionary;
    }

    public Map<String, Integer> getDictionnary2() {
        return dictionary2;
    }

    public void addElement(String value) {
        // Utiliser computeIfAbsent pour éviter deux appels de get
        this.dictionary2.computeIfAbsent(value, v -> {
            dictionary.put(key, v);
            return key++;
        });
    }

    public void constructDic(Statement st) {
        addElement(st.getSubject().toString());
        addElement(st.getObject().toString());
        addElement(st.getPredicate().stringValue());
    }

    public int getKey(String value) {
        return dictionary2.getOrDefault(value, -1);
    }

    public String getValue(int key) {
        return dictionary.get(key);
    }

    public void displayDictionary() {
        dictionary.forEach((k, v) -> System.out.println(k + " " + v));
    }
}
*/
