package hadoop_mapreduce_demos.qq_friends;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 求共同好友
 * 第一步，map:友-人
 * reduce:友-人，人
 * 第二部,map：先将人排序，然后二维遍历人，塞入key,友作为value
 * reduce:某人-某人：友,友
 * @author Win7
 *
 */
public class FindQQFriends {
	static class FindFriends_Step1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(":");
			String people = fields[0];
			for(String friend:fields[1].split(",")){
				context.write(new Text(friend), new Text(people));
			}
		}
	}
	static class FindFriends_Step1Reducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text friend, Iterable<Text> peoples,Context context)
				throws IOException, InterruptedException {
			StringBuffer people = new StringBuffer();
			for(Text person:peoples){
				people.append(person.toString()).append(",");
			}
			context.write(friend, new Text(people.toString()));
		}
	}
	static class FindFriends_Step2Mapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\t");
			v.set(fields[0]);
			String[] peoples = fields[1].split(",");
			Arrays.sort(peoples);
			for(int i=0;i<peoples.length-2;i++){
				for(int j=i+1;j<peoples.length-1;j++){
					k.set(peoples[i]+"-"+peoples[j]);
					context.write(k, v);
				}
			}
		}
	}
	static class FindFriends_Step2Reducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer friends = new StringBuffer();
			for(Text friend:values){
				friends.append(friend.toString()).append(",");
			}
			context.write(key, new Text(friends.toString()));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setMapperClass(FindFriends_Step1Mapper.class);
//		job.setReducerClass(FindFriends_Step1Reducer.class);
//		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/friends/input"));
//		Path outpath = new Path("G:/hadoop_test/friends/output2");
		job.setJarByClass(FindQQFriends.class);
		job.setMapperClass(FindFriends_Step2Mapper.class);
		job.setReducerClass(FindFriends_Step2Reducer.class);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/friends/output2"));
		Path outpath = new Path("G:/hadoop_test/friends/output3");
		if(FileSystem.get(conf).exists(outpath)){
			FileSystem.get(conf).delete(outpath,true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
