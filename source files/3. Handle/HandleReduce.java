/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the country.txt, countrylanguage.txt and select name of the countries where english is the official language
Input file : country.txt, countrylanguage.txt
Output : ans3.txt file

This class sets up a reducer job for query 3
--All the <key, value> pairs are received from all mapper jobs.
--Reads each line of country.txt are split based on delimiter ‘,’ and first two columns which represent country code and respective country name are stored in an array.
--Join operation performed on received <key, value> pairs and stored columns to extract required results. (Natural Join Operation)
--Results are written to output file.
-------------------------------------------------------------------------------------------------------------------------------------
*/
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class HandleReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator ite = values.iterator();
			int counter = 0;
			String realcountry = "";
			while (ite.hasNext()) {
				String line = ite.next().toString();
				
				if (!line.isEmpty()) {
					realcountry = line;
				}
				counter++;
			}
			if (counter > 1) {
				context.write(new Text(realcountry), new Text(""));
			}
			
		}
	}