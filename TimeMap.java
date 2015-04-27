package com.umd.project;
/*author: Vamsidhar Kasireddy
 * functionality: This is the first mapper in the project. It take a WikiStat file and a file's date as input. It will then output a languageCode+PageName key with a Date+View-count value.
 Sample Input :
 Language-Code (2 character abbreviations, ex: en)
 Page-Name
 View-Count
 Content size
 in that order, delimited by spaces. The project requires the wikiStats dump files to be placed in the specified input directory.
 File Names are in the format pagecounts-yyyymmdd-hhmmss of which yyyymmdd will be used as value in the First Mapper
 Here some sample input from the file which adheres to the above naming convention:
 en MichealJordan 3672 20081
 en MichealJackson 3548 22866
 
 Sample Output :
 Language-Code(Key) Page-name(key) Date(value) View-count(value)
 en MichaelJordan 20140601 3672

 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TimeMap extends Mapper<LongWritable, Text, Text, Text> {
    private final static Text one = new Text();//Initializes the Value for first Map.
	private Text Merge_key = new Text();//Initializes the Key for first Map.

	/*
	  This method is used for reading files from WikiStat and creates a Key and a Value for the given Context.
    */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();//converts value to String variable.
		//Retrives the File Name and the File Date and saves into
		//thier own variables.
		String part[] = line.split(" ");//Splits input data into LanguageCode, Page Name, and Views. Then places into the array Part.
		//Checks to see if input was read in correctly.
		if (part.length == 4 && part[0].length() == 2) {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		String fileDate = fileName.substring(11,19);
		    //Checks if Language code has a length of 2.
			    //Creates the Key for the current input, which is LanguageCode + PageName
			    Merge_key.set(part[0]+"\t"+part[1]);//Prepares Key for Map setup.
			    one.set(fileDate+" "+part[2]);//Prepares Value for Map setup.
			    context.write(Merge_key, one);//Sets up a new Key, Value for the context.

		}
	}
}
