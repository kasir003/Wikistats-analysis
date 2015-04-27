/*
 * author: Kris Samuelson
 * functionality: takes input from third mapper and outputs top N pages per top M languages in descending order of spike as well as count of unique pages for each language
 * sample input:
 * constant-key (key) language-code(value) (page-name(value) spike(value)) or (unique-pages(value))
 * shouldBeSameAlways en apple 999
 * shouldBeSameAlways en orange 88
 * shouldBeSameAlways en banananana 23
 * shouldBeSameAlways en 3
 *sample output: 
 * en apple 999
 * en orange 88
 * en banananana 23
 * en 3
 */

package com.umd.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastReducer extends Reducer<Text, Text, Text, Text> {
	public Text pageKey = new Text();
	public Text pageValue = new Text();
	public List<String> listValue = new ArrayList<String>();
	public Integer intValue;
    
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		org.apache.hadoop.conf.Configuration conf3 = context.getConfiguration();
		int mLang = Integer.parseInt(conf3.get("mLang"));

		List<String> topLang = new ArrayList<String>(); //list of languages
		Map<Integer, List<String>> filteredLang = new HashMap<Integer, List<String>>(); //stores unique page count for a language as well as top N pages for that languages
		
		for (Text val : values) {
			String keyValuePair[] = val.toString().split("\t");
			String valueLang = keyValuePair[1];
			topLang.add(val.toString());
            //if it's a page input line, insert it into filteredLang
			if (!(valueLang.toString().split(" ").length == 2)) {
				filteredLang.put(Integer.parseInt(valueLang), topLang);
				topLang = new ArrayList<String>();
			}
		}
        //sort languages by unique page count
		Map<Integer, List<String>> sortedMap = new TreeMap<Integer, List<String>>(
				new Comparator<Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return o2.compareTo(o1);
					}
				});
		sortedMap.putAll(filteredLang);
		int counter = 0;
        //for M languages, output pages and unique page count
		for (int i : sortedMap.keySet()) {
			counter++;
			List<String> lang = sortedMap.get(i);
            //for N pages in that language, output the page (last entry is language-code and unique-count)
			for (String langLine : lang) {
				String entry[] = langLine.split("\t");
				pageKey.set(entry[0]);
				pageValue.set(entry[1]);
				context.write(pageKey, pageValue);
			}
			if (counter == mLang)
				break;
		}
	}
}
