/* 
 * author: Mazin Jindeel
 * functionality: second reducer, takes input from second mapper and outputs languagecode as key with page-name and spike as values along with language-code and unique count of pages for that language-code
 * sample input:
 * language-code (key) page-name(value) spike(value)
 * en apple 999
 *
 * sample output: 
 * language-code (key) page-name(value) spike(value) (sorted in descending order)
 * en apple 999
 * en orange 88
 * en banananana 23
 * en 3
 */

package com.umd.project;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueCount extends Reducer<Text, Text, Text, Text> {
	
    /* 
     * method finds top N pages for a language and unique page count for the language
     */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		org.apache.hadoop.conf.Configuration conf2 = context.getConfiguration();
		int nPages = Integer.parseInt(conf2.get("nPages"));
		 HashMap<String, Integer> topPages = new HashMap<String, Integer>(); //unsorted list of pages with page-name as key and spike as value
		 ValueComparator byValue =  new ValueComparator(topPages);
		Map<String, Integer> spikesInDesc = new TreeMap<String, Integer>(byValue); //sort topPages by spike in descending order
		Text newValue = new Text();
		int uniqueCount = 0;
        //increment uniquecount, put each input into topPages
		for (Text val : values) {
			uniqueCount++;
			String line = val.toString();
			String parts[] = line.split(" ");
			String alterKey = parts[0];
			int alterValue = Integer.parseInt(parts[1]);
			topPages.put(alterKey,alterValue);
            //after N + 1 pages have been added to the hashmap, sort in descending order of spike and remove last key
		if (topPages.size()>nPages)
		{
			spikesInDesc.putAll(topPages);
			topPages.remove(((TreeMap<String, Integer>) spikesInDesc).lastKey());
			spikesInDesc.clear();
		}

		}
		
		spikesInDesc.putAll(topPages); //sort final N entries
	//ouput top N pages for the language
	for(Map.Entry<String,Integer> entry : spikesInDesc.entrySet()) {
		  String mapKey = entry.getKey(); //set new key
		   Integer mapValue= entry.getValue();
		   newValue.set(mapKey+" "+mapValue);//set new value
		   context.write(key,newValue);//write page
		  
		}
		//output page-count for the language
		newValue.set(String.valueOf(uniqueCount));
		context.write(key,newValue);
		

	}

	//compare entries by value
	class ValueComparator implements Comparator<String> {

	    Map<String,Integer> base;
	    public ValueComparator(Map<String,Integer> base) {
	        this.base = base;
	    }

	    @Override
	    public int compare(String a, String b) {
	        if (base.get(a) >= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } 
	    }
 
}
}
