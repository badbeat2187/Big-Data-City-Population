/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the city.txt and select name of the cities and corresponding district
Input file : city.txt 
Output : ans2.txt file

This class sets up a reducer job for query 2
--All the <key, value> pairs are received from all mapper jobs.
--Columns are filtered based on required condition.(Project Operation)
--Result columns are written to output file.
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

public class ProjectionReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator ite = values.iterator();
			String total = "";
			while (ite.hasNext()) {
				String dis = ite.next().toString();
				total += dis;
			}
			context.write(new Text(key), new Text(total));
		}
	}