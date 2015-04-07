package org.yottabase.billing.es1.onereducer;

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
		
		String inputJob = args[0];
		String outputJob = args[1];
		
		FileSystem.deleteDirectory(new File(outputJob));
		
		runJob(inputJob, outputJob);
	}
	
	
	private static void runJob(String inputPath, String outputPath) throws Exception{
		Job job = new Job(new Configuration(), "SimpleBilling");
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setJarByClass(Main.class);
		job.setMapperClass(SimpleBillingMapper.class);
		job.setCombinerClass(SimpleBillingCombiner.class);
		job.setReducerClass(SimpleBillingReducer.class);
		
		/*
		 *  soluzione necessaria per produrre un solo file di output con ordinamento globale
		 *  la soluzione scala sugli scontrini, ma non sul numero di prodotti
		 */
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
	}
}