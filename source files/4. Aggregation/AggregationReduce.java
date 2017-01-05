/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the city.txt and find how many cities each district has
Input file : city.txt
Output : ans4.txt file

This class sets up a reducer job for query 4
--All the <key, value> pairs are received from all mapper jobs.
--Reads each line of country.txt are split based on delimiter ‘,’ and first two columns which represent country code and respective country name are stored in an array.
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

public class AggregationReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			IntWritable sum = new IntWritable(0);
			for (IntWritable val:values) {
				sum = new IntWritable(val.get() + sum.get());
			}
			context.write(key, sum);
		}
	}