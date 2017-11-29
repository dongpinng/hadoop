package hadoop_mapreduce_demos.word_file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordFile_Step {
	static class WordFile_Step1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		Text k = new Text();
		IntWritable num1 = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\s");
			String file_name = ((FileSplit)context.getInputSplit()).getPath().getName();
			for(String field:fields){
				k.set(field+"-"+file_name);
				context.write(k, num1);
			}
		}
	}
	static class WordFile_Step1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable num : values){
				count += num.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	static class WordFile_Step2Mapper extends Mapper<LongWritable, Text,Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("-");
			k.set(fields[0]);
			v.set(fields[1]);
			context.write(k, v);
		}
	}
	static class WordFile_Step2Reducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuffer line = new StringBuffer() ;
			for(Text v : values){
				line.append(v.toString()).append("&");
			}
			context.write(key, new Text(line.toString()));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
//		job.setMapperClass(WordFile_Step1Mapper.class);
//		job.setReducerClass(WordFile_Step1Reducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/wordcount/input"));
//		Path outpath = new Path("G:/hadoop_test/wordcount/output2");
		job.setJarByClass(WordFile_Step.class);
		job.setMapperClass(WordFile_Step2Mapper.class);
		job.setReducerClass(WordFile_Step2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/wordcount/output2"));
		Path outpath = new Path("G:/hadoop_test/wordcount/output3");
		if(FileSystem.get(conf).exists(outpath)){
			FileSystem.get(conf).delete(outpath,true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
