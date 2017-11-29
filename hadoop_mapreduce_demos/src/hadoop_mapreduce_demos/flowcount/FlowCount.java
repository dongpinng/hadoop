package hadoop_mapreduce_demos.flowcount;

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
 * 上行，下行，总流量统计：
 * map:Text,实体bean
 * reduce:实体bean，NUll
 * @author Win7
 *
 */
public class FlowCount {
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
			InfoBean ibean = new InfoBean();
			Text k = new Text();
			@Override
			protected void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
				String[] fields = value.toString().split("\t");
				k.set(fields[0]);
				ibean.set(new LongWritable(Long.parseLong(fields[1])), new LongWritable(Long.parseLong(fields[2])));
				context.write(k, ibean);
			}
	} 
	static class FlowCountReducer extends Reducer<Text, InfoBean, Text, InfoBean>{
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values,Context context)
				throws IOException, InterruptedException {
			InfoBean bean = new InfoBean();
			Long sum_upflow = 0L;
			Long sum_downflow = 0L;
			for(InfoBean ibean:values){
				sum_upflow += ibean.getUpflow().get();
				sum_downflow += ibean.getDownflow().get();
			}
			bean.set(new LongWritable(sum_upflow), new LongWritable(sum_downflow));
			context.write(key, bean);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowCount.class);
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/flowcount/input"));
		Path outpath = new Path("G:/hadoop_test/flowcount/output");
		if(FileSystem.get(conf).exists(outpath)){
			FileSystem.get(conf).delete(outpath,true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
