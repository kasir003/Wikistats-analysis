package com.umd.project;
/*author: Vamsidhar Kasireddy
 *functionality: This is the first reducer of the project. Its job is to take in the output from TimeMap.java which is a Map with a Key: LanguageCode+PageName with a value of: Date+View-count. This Reducer will output the LanguageCode+PageName as a key and with spike as the value.
 * sample input:
 * languagecode(key)   pageName(key) date(value) view-count(value)
 * en   apple 20140601 9001
 *
 * sample output: 
 * languagecode(key) pagename(key) spike(value)
 * en apple 900
 */
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//
public class Reduce extends Reducer<Text, Text, Text, IntWritable> {

    private final SimpleDateFormat SDF = new SimpleDateFormat("yyyymmdd");//Initializes a Date Format to be used.
    public static final Integer totaldays = 60;//Total number of Days.
    /*
      This method reduces the output from the Mapper TimeMap.java.
     */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException, NullPointerException {
		org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
		int timePeriod = Integer.parseInt(conf.get("oPeriod"));//Retrieves 
		int groupNum = totaldays - timePeriod + 1;//Determines the Group Number.
		int groupMaxValue;//Maximum Value or views for page, used for finding spike.
		int groupMinValue;//Minimum Value of views for page, used for finding spike.
		int totalSpike = 0; //Initializes the total number of spikes at 0.
		Map<String, Integer> dayRecordMax = new HashMap<String, Integer>();//Initializes a new HashMap for Maximum views for a page in a day.
		Map<String, Integer> dayRecordMin = new HashMap<String, Integer>();//Initializes a new HashMap for Minimum view for a page in a day.
		Map<Date, String> daysInAsc = new TreeMap<Date, String>();//Initializes a new TreeMap to sort and store days in ascending order
		//Goes through all values from the Map.
		for (Text val : values) {

		    String[] dateAndAccess = val.toString().split(" ");//Splits up current Value of Map and stores into array.
		    String date = dateAndAccess[0];//Stores the Date into its own variable.
		    int access = Integer.parseInt(dateAndAccess[1]);//Stores the Views into its own variable and converts to integer.
		    if (dayRecordMax.containsKey(date)) {//Checks to see if Date of current value is in the dayRecordMax HashMap as a key.
			if (access > dayRecordMax.get(date))//Checks to see if current view-count is greater then the previously recorded view-count.
			    dayRecordMax.put(date, access);//If this is true, then view-count replaces the old view-count of date.
			} else {
			    dayRecordMax.put(date, access);//Adds new date/view-count to Max HashMap.
			}

		    if (dayRecordMin.containsKey(date)) {//Checks to see if Date of current is in the dayRecordMin HashMap as a key.
			if (access < dayRecordMin.get(date))//Checks to see if current view-count is greater then the previously recorded view-count.
			    dayRecordMin.put(date, access);//If this is true, then view-count replaces the old view-count of date.
			} else {
			    dayRecordMin.put(date, access);//Adds new date/view-count to Min HashMap.
			}

			try {
			    if (!daysInAsc.containsKey(getDateWithAddDays(date, 0)))//Checks if given Date is in daysInAsc as a key.
				daysInAsc.put(getDateWithAddDays(date, 0), date);//If it is not, then it is added to daysInAsc.
			} catch (ParseException e) {//Catches a ParseException.
				
				e.printStackTrace();
			}

		}
		Date firstDate = ((TreeMap<Date, String>) daysInAsc).firstKey();//Is set to the first value within the TreeMap.

		Set<Date> dates = daysInAsc.keySet();//set the keys to iterator

		for (Date date : dates) {//loops through for all Date Keys in daysInAsc.

			try {/*
                  Divides the input data into groups based on O day time period
                  */
				if (date.before(getDateWithAddDays(getDateInString(firstDate),
								   groupNum))) {//Checks if current Date is before the date(firstDate+groupNum).

				    Date groupDate = date;//sets current date as groupDate.
					groupMaxValue = dayRecordMax
					    .get(getDateInString(groupDate));//Gets Maximum view value matching given groupDate.
					groupMinValue = dayRecordMin
					    .get(getDateInString(groupDate));//Gets Minimum view value matching given groupDate.
					for (int i = 1; i < timePeriod; i++) {

					    String currentDate = daysInAsc.get(groupDate);//Sets current Date, based on groupDate.

						if(currentDate!=null)
						{
							String nextDate = getDateInString(getDateWithAddDays(currentDate, 1));//Sets next day's date.
											

						/* 
                         Finding the maximium view-count in the current O day time Period
                         */
                            if (dayRecordMax.containsKey(nextDate)
							& dayRecordMax.containsKey(currentDate)) {
							if (dayRecordMax.get(nextDate) > groupMaxValue) {
								groupMaxValue = dayRecordMax.get(nextDate);
							}
						}
                            /*
                             Finding the minimum view-count in the current O day time Period
                             */
						if (dayRecordMin.containsKey(nextDate)
								&& dayRecordMin.containsKey(currentDate)) {
							if (dayRecordMin.get(nextDate) < groupMinValue) {
								groupMinValue = dayRecordMin.get(nextDate);
							}
						}
							}
						else
							break;
						groupDate = getDateWithAddDays(
									       daysInAsc.get(groupDate), 1);//Increments current day by one

					}
					int groupSpike = groupMaxValue - groupMinValue;//Determines the Spike based on the group's Maximum - Minimum.
					//Checks if the new Group Spike is larger then the Total Spike. If true Group Spike becomes new Total Spike.
					if (groupSpike > totalSpike)
						totalSpike = groupSpike;
				}
			} catch (ParseException e) {//Catches ParseException.
				e.printStackTrace();
			}

		}
		context.write(key, new IntWritable(totalSpike));//This adds the new Key, Value to the current Context.
	}
	/*
	  This method increments the date by the number of days.
	  @param date This is the String that must be converted to a Date.
	  @param days This is the number of days to be added to the given Date.
	  @returns Date This returns the new date after the given number of days have passed.
	 */
	public Date getDateWithAddDays(String date, int days)
			throws ParseException, NullPointerException {
		Date formattedDate = SDF.parse(date);
		Calendar cal = Calendar.getInstance();
		cal.setTime(formattedDate);
		cal.add(Calendar.DATE, days);
		return cal.getTime();
	}
	/*
	  This method is used for converting a Date into String
	  @param Date This is the date that must be converted to String.
	  @return String Returns the String representation of a Date.
	 */
	public String getDateInString(Date date) {
		return SDF.format(date);
	}
}
