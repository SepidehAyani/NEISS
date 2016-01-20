
package main.java;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

public class NEISSDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {

			System.out.printf("Usage: NEISSDriver <input dir> <output dir>\n");

			return -1;

		}

		Job job = new Job(getConf());

		job.setJarByClass(NEISSDriver.class);

		job.setJobName("NEISS");

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(NEISSMapper.class);

		job.setReducerClass(NEISSReducer.class);

		job.setCombinerClass(NEISSReducer.class);
		
		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		if (job.getCombinerClass() == null) {

			throw new Exception("Combiner not set");

		}

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(), new NEISSDriver(), args);

		System.exit(exitCode);

	}

}
