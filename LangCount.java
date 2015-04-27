/*
 * author: Mazin Jindeel
 * functionality: takes output of first reducer and outputs language-code as key and page-name + spike as the value
 *
 * sample input:
 * languagecode(key) pagename(key) spike(value)
 * en apple 900
 *
 * sample output: 
 * language-code (key) page-name(value) spike(value)
 * en apple 999
 */
package com.umd.project;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LangCount extends Mapper<LongWritable,Text,Text,Text>{
	private Text newKey = new Text();
	private Text newValue = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//split input into
		String line = value.toString();
		String[] parts = new String[3];
		parts = line.split("\t");
        //check if input string was appropriately formatted
		if(parts.length==3)
		{
		String webPage = parts[1];
		newValue.set(webPage+" "+parts[2]);//set new value as page-name and spike
		newKey.set(parts[0]);//set key as language code
		context.write(newKey, newValue);
		}
	
	}
	
	}

