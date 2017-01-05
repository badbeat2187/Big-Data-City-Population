/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the country.txt, countrylanguage.txt and select name of the countries where english is the official language
Input file : country.txt, countrylanguage.txt
Output : ans3.txt file
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

public class Handle extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {

		Job conf = new Job(getConf());
		conf.setJarByClass(Handle.class);
		conf.setconfName("Handle");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(HandleMap.class);
		conf.setReducerClass(HandleReduce.class);

		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("data/country.txt"));
		FileInputFormat.addInputPath(conf, new Path("data/countrylanguage.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("ans3"));

		boolean success = conf.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Handle(), args);
		System.exit(ret);
	}

}