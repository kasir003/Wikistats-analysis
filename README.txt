Overview:

The project identifies Wikipedia pages of a language that have large increases in activity (spikes) over a given period of days within the included 60 day input data using Hadoop MapReduce framework on Itasca. 

How to run the project
- download and untar desired wikiStats data
- configure and run runit.sh
- check the output directory! 

The project takes three command line arguments: 
N which indicates the length of the list of top N spike pages per language. 
M, which indicates the length of the list of top M languages by unique page count. 
O, which indicates the length of periods of days. 

Details: 

 * WikiStats Data: The input should be unadulterated WikiStats data, downloaded from http://dumps.wikimedia.org/other/pagecounts-raw/. Lines of input are formatted in the form: 
Language-Code (2 character abbreviations, ex: en)
Page-Name
View-Count
Content size
in that order, delimited by spaces. The project requires the wikiStats dump files to be placed in the specified input directory.
File Names are in the format pagecounts-yyyymmdd-hhmmss of which yyyymmdd will be used as value in the First Mapper 
Here some sample input from the file which adheres to the above naming convention:   
en MichealJordan 3672 20081
en MichealJackson 3548 22866
en Rihanna 3356 23638
en Justin 3241 24976
en Brady 1541 29554
en Eric 2583 21022
en David 3743 21202
en Stephen 3628 21388
en Ben 3116 26353
en Danny 1931 24649 
 
* output: output will be the top N pages of M languages within an O day period. More specifically, the top N pages for each language will be listed in descending order based on spike, followed by a page count for that language.
Here is some sample output for one language: 
en	Ben 2982
en	Brady 2969
en	Danny 2963
en	Boxing 2945
en	Hockey 2909
en	20
* Requirements: Hadoop cluster running Hadoop 1.03. If you are sharing resources with other users, make sure to allocate enough cores and a long enough runtime for your job to complete. 

* How it Works: The project is made up of 3 MapReduce jobs. The first Mapper (TimeMap.java) will take in files, retrieving the date from the filenames. The output is language-code + Page Name as key and date + view-count as the value. This goes to the the first reducer(Reduce.java), which will output the language-code + page-name as the key and maximum spike of the Key. Then, the second mapreduce cycle (LangCount.java) Mapper will run, taking the output of the first reducer and outputs language-code as the key and page-name + spike as the value. The second reducer (UniqueCount.java) will output the top N page-names with spikes in descending order for each language-code along with count of unique pages for each language-code, directly under the list of N pages. The third, and final mapper (TopPages.java) takes in the results of the second reducer and assigns a default key to all the values which are the top N page-name of each language-code followed by the count of unique pages. This goes into the aptly named final reducer (LastReduce.java), which outputs the top N page-name for the top M languages, which is the final output. 

Contributors:

- Kris Samuelson - 	Third MapReduce cycle (TopPages.java and LastReduce.java)
- Mazin Jindeel - 	Second MapReduce cycle (LangCount.java and UniqueCount.java)  
- Vamsidhar Kasireddy - First MapReduce cycle (TimeMap.java and Reduce.java) 
- Group effort - 	Initial research, Readme file, downloading input files, chaining jobs, and WikiStats.java