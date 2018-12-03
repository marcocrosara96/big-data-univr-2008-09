package ex2b;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int
 *
 **/
public class StringToIntAssociativeArrayWritable implements Writable {
	private HashMap<String, Integer> hm; //DIZIONARIO

	public StringToIntAssociativeArrayWritable(){
		hm = new HashMap<String, Integer>();
	}

	public void clear() {
		hm.clear();
	}

	public void put(String key, int value){
		if(hm.containsKey(key))
			hm.put(key, hm.get(key) + value);
		else
			hm.put(key, value);
	}

	public String toString() {
		return hm.toString();
	}

	//SERIALIZZAZIONE ---- DESERIALIZZAZIONE DEL DATO SU E DA DISCO

	/**
	 * String s ---> | #numerocaratteri | byte stringa (ricordarsi del carattere invisibile chiusura stringa)
	 * Il nostro dizionario è fatto nel seguente modo (key:String, value:Integer), (String, Integer), (String, Integer) ....
	 * Dizionario d ---> | #numerovocidizionario | ((#numcaratteri_1a_key | caratteri_1a_key / valore:Integer))*
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(hm.size());
		for (String key : hm.keySet()) {
			out.writeInt(key.length());
			out.writeBytes(key);
			out.writeInt(hm.get(key));
		}
	}

	/**
	 * DESERIALIZZAZIONE => Gli dico come leggere i dati da disco
	 * @param in
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int N = in.readInt();
		for (int i = 0; i < N; i++) {
			int keyl = in.readInt();
			String key = "";
			for (int j = 0; j < keyl; j++){
				char c = (char)in.readByte();
				key += c;
			}
			int value = in.readInt();
			hm.put(key, value);
		}
	}

	public void sum(StringToIntAssociativeArrayWritable h) {
		for (String s : h.hm.keySet()) {
			this.hm.put(s, h.hm.get(s));
		}
	}

}
