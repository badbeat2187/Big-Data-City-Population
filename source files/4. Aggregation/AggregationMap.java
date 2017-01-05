/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the city.txt and find how many cities each district has
Input file : city.txt
Output : ans4.txt file

This class sets up a mapper job for query 3
--Read each line of city.txt are split based on delimiter ‘,’ and stored in an array as separate columns.
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

public class AggregationMap extends Mapper<Object, Text, Text, IntWritable> {
		
		private IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] list = line.split(",");
			String district = list[3];
			context.write(new Text(district), one);
		}
	}