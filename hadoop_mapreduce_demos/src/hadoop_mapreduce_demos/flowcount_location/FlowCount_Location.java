package hadoop_mapreduce_demos.flowcount_location;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 上行，下行，总流量统计按照地区： map:Text,实体bean reduce:Text，InfoBean
 * 
 * @author Win7
 *
 */
public class FlowCount_Location {
	static class FlowCount_LocationMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
		InfoBean ibean = new InfoBean();
		Text k = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\t");
			k.set(fields[0]);
			ibean.set(new LongWritable(Long.parseLong(fields[1])), new LongWritable(Long.parseLong(fields[2])));
			context.write(k, ibean);
		}
	}
	static class Flow_Partition extends Partitioner<Text, InfoBean>{
		static Map<String,Integer> provinceMap = new HashMap<String,Integer>();
		static{
			provinceMap.put("135",0);	
			provinceMap.put("136",1);
			provinceMap.put("137",2);
			provinceMap.put("138",3);
		}
		@Override
		public int getPartition(Text key, InfoBean value, int numPartitions) {
			Integer code = provinceMap.get(key.toString().substring(0, 3));
			return code==null?4:code;
		}

		
	}
	static class FlowCount_LocationReducer extends Reducer<Text, InfoBean, Text, InfoBean> {
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values, Context context)
				throws IOException, InterruptedException {
			InfoBean bean = new InfoBean();
			Long sum_upflow = 0L;
			Long sum_downflow = 0L;
			for (InfoBean ibean : values) {
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
		job.setJarByClass(FlowCount_Location.class);
		job.setMapperClass(FlowCount_LocationMapper.class);
		job.setReducerClass(FlowCount_LocationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		job.setPartitionerClass(Flow_Partition.class);
		job.setNumReduceTasks(5);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/flowcount/input"));
		Path outpath = new Path("G:/hadoop_test/flowcount/output");
		if (FileSystem.get(conf).exists(outpath)) {
			FileSystem.get(conf).delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
