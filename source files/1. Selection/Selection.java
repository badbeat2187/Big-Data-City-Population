/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the city.txt and select the city whose
population is larger than 300,000
Input file : city.txt 
Output : ans1.txt file is generated showing the results of question in the following format
City: (city name)
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

public class Selection extends Configured implements Tool {
@Override
	public int run(String[] args) throws Exception {

		Job conf = new Job(getConf());
		conf.setJarByClass(Selection.class);
		conf.setconfName("Selection");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(SelectionMap.class);
		conf.setReducerClass(SelectionReduce.class);

		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("data/city.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("ans1"));

		boolean success = conf.waitForCompletion(true);

		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Selection(), args);
		System.exit(ret);
	}
}