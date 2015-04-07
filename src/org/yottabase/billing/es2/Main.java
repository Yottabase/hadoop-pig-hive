package org.yottabase.billing.es2;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.yottabase.utils.FileSystem;

public class Main {

	public static void main(String[] args) throws Exception {

		String inputJob1 = args[0];
		String outputJob1 = args[1] + "/job1";
		
		String inputJob2 = outputJob1;
		String outputJob2 = args[1] + "/job2";
		
		FileSystem.deleteDirectory(new File(outputJob1));
		FileSystem.deleteDirectory(new File(outputJob2));
		
		runJob1(inputJob1, outputJob1);
		runJob2(inputJob2, outputJob2);
		
	}
	
	private static void runJob1(String inputPath, String outputPath) throws Exception{
		
		Job job = new Job(new Configuration(), "quarter_aggregation_es2_job1");
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setJarByClass(Main.class);
		job.setMapperClass(QuarterAggregationMapper.class);
		job.setCombinerClass(QuarterAggregationReducer.class);
		job.setReducerClass(QuarterAggregationReducer.class);
		
		// Configuration
		job.setOutputKeyClass(ProductByMounth.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
	}
	
	private static void runJob2(String inputPath, String outputPath) throws Exception{
		
		Job job = new Job(new Configuration(), "quarter_aggregation_es2_job2");
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setJarByClass(Main.class);
		job.setMapperClass(InlineQuarterAggregationMapper.class);
		job.setReducerClass(InlineQuarterAggregationReducer.class);

		// Configuration
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CountByMounth.class);
		job.waitForCompletion(true);
		
	}
	
}