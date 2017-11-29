package hadoop_mapreduce_demos.flowcount_flowdesc;

import java.io.IOException;

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
 * 上行，下行，总流量排序统计：
 * map:Text,实体bean
 * reduce:实体bean，NUll
 * @author Win7
 *
 */
public class FlowCount_FlowDesc {
	static class FlowCount_FlowDescMapper extends Mapper<LongWritable, Text, InfoBean, Text>{
			InfoBean ibean = new InfoBean();
			Text k = new Text();
			@Override
			protected void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
				String[] fields = value.toString().split("\t");
				k.set(fields[0]);
				ibean.set(new LongWritable(Long.parseLong(fields[1])), new LongWritable(Long.parseLong(fields[2])));
				context.write(ibean, k);
			}
	} 
	static class FlowCount_FlowDescReducer extends Reducer<InfoBean, Text, Text, InfoBean>{
		@Override
		protected void reduce(InfoBean key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			context.write(values.iterator().next(),key);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowCount_FlowDesc.class);
		job.setMapperClass(FlowCount_FlowDescMapper.class);
		job.setReducerClass(FlowCount_FlowDescReducer.class);
		job.setMapOutputKeyClass(InfoBean.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/flowcount/output"));
		Path outpath = new Path("G:/hadoop_test/flowcount/outputdesc");
		if(FileSystem.get(conf).exists(outpath)){
			FileSystem.get(conf).delete(outpath,true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
