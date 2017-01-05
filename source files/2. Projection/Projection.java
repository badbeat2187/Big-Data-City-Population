/**
-------------------------------------------------------------------------------------------------------------------------------------
Author : Swami Nikhil Nagendra
Subject : Adavanced Database Systems, Final Project
This Program is used to parse the city.txt and select name of the cities and corresponding district
Input file : city.txt 
Output : ans2.txt file
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

public class Projection extends Configured implements Tool {


	@Override
	public int run(String[] args) throws Exception {

		Job conf = new Job(getConf());
		conf.setJarByClass(Projection.class);
		conf.setconfName("Projection");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(ProjectionMap.class);
		conf.setReducerClass(ProjectionReduce.class);

		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("data/city.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("ans2"));

		boolean success = conf.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Projection(), args);
		System.exit(ret);
	}
}