package hadoop_mapreduce_demos.wordcount;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 * 单词统计：
 * map：单词,1
 * reduce：遍历value+1
 * @author Win7
 *
 */
public class WordCount {
	static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		IntWritable num = new IntWritable(1);
		Text k = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("[\\s\\.,]");
			for(String field:fields){
				k.set(field);				
				context.write(k, num);
			}
		}
	}
	static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum_count = 0;
			for(IntWritable count : values){
				sum_count += count.get(); 
			}
			context.write(key, new IntWritable(sum_count));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/wordcount/input"));
		Path outpath = new Path("G:/hadoop_test/wordcount/output");
		if(FileSystem.get(conf).exists(outpath)){
			FileSystem.get(conf).delete(outpath,true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
