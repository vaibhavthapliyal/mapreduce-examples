package com.vaibhav.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostViewedMoviesJob {
	public static class MovieMapper extends Mapper<Text, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text movieId = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			String fields[] = value.toString().split("::");

			movieId.set(fields[0]);

			context.write(movieId, one);
		}
	}

	public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class TopNMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text movieId = new Text();

		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			System.out.println("Key--->" + key);
			System.out.println("Value--->" + value);

			context.write(key, value);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(MostViewedMoviesJob.class);
	    job.setMapperClass(MovieMapper.class);
	    job.setReducerClass(MovieReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/home/vaibhav/Downloads/ml-1m/ratings.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("/home/vaibhav/Downloads/ml-1m/new4"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
/*		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setMapperClass(MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		job.setJarByClass(MostViewedMoviesJob.class);
		job.setJobName("Movie_view_count");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/home/vaibhav/Downloads/ml-1m/ratings.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/vaibhav/Downloads/ml-1m/new4"));

		job.waitForCompletion(true);
		
		System.out.println("first job completed");
*/		/*Job job1 = Job.getInstance(conf);

		job1.setMapperClass(MovieMapper.class);
		job1.setReducerClass(MovieReducer.class);
		job1.setJarByClass(MostViewedMoviesJob.class);
		job1.setJobName("Top 10 Movies");
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path("/home/vaibhav/Downloads/ml-1m/new4"));
		FileOutputFormat.setOutputPath(job1, new Path("/home/vaibhav/Downloads/ml-1m/new5"));

		job1.waitForCompletion(true);
*/	}

}
