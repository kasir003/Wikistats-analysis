/*
 * author: Kris Samuelson
 * functionality: takes input from the second reducer and outputs the input key+value pair as the value with a constant key in order to send all records to the same reducer
 * sample input: 
 * language-code (key) page-name(value) spike(value) (sorted in descending order)
 * en apple 999
 * en orange 88
 * en banananana 23
 * en 3
 * sample output:
 * constant-key (key) language-code(value) (page-name(value) spike(value)) or (unique-pages(value))
 * shouldBeSameAlways en apple 999
 * shouldBeSameAlways en orange 88
 * shouldBeSameAlways en banananana 23
 * shouldBeSameAlways en 3
 */
package com.umd.project;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopPages extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException,
			InterruptedException {
		Text constantKey = new Text();
		constantKey.set("shouldBeSameAlways"); //set constant key
		context.write(constantKey, value);
	}

}

