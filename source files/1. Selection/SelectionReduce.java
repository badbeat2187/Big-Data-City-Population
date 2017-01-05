/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the city.txt and select the city whose
population is larger than 300,000
Input file : city.txt 
Output : ans1.txt file is generated showing the results of question in the following format
City: (city name)

This class sets up a reducer job for query 1
--All the <key, value> pairs are received from all mapper jobs.
--Rows are filtered based on condition population > 300,000.(Select Operation)
--Result rows are written to output file.
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

	public class SelectionReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(new Text(key), new Text(""));
		}
	}
