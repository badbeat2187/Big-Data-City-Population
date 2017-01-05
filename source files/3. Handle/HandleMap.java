/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the country.txt, countrylanguage.txt and select name of the countries where english is the official language
Input file : country.txt, countrylanguage.txt
Output : ans3.txt file

This class sets up a mapper job for query 3
--Reads each line of countrylanguage.txt are split based on delimiter ‘,’ and stored in an array as separate columns.
--First two columns which represent country code and respective language are mapped and sent as <key, value> pair to reducer.
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

public class HandleMap extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] list = line.split(",");

			if (list.length > 4) { 
				String countryCode = list[0];
				String countryName = list[1];
				context.write(new Text(countryCode), new Text(countryName));
				System.out.println(list[0] + " " + list[1]);
			} else { 
				String countryCode = list[0];
				String language = list[1];
				if (language.compareTo("English") == 0) {
					System.out.println(countryCode + " " + language);
					context.write(new Text(countryCode), new Text(""));
				}
			}
		}
	}