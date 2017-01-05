/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the city.txt and select name of the cities and corresponding district
Input file : city.txt 
Output : ans2.txt file

This class sets up a mapper job for query 2
--Reads each line of city.txt are split based on delimiter ‘,’ and stored in an array as separate columns.
--Column 2 and 4 are mapped and sent as <key, value> pair to reducer.
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

public class ProjectionMap extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] list = line.split(",");

			String city = list[1];
				String district = list[3];

				context.write(new Text(city), new Text(district));
		}
	}