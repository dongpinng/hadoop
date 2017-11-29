package hadoop_mapreduce_demos.MaxOrder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxOrder {
	
	static class MaxOrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
		OrderBean obean = new OrderBean();
		NullWritable n = NullWritable.get();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] items = value.toString().split(",");
			obean.set(items[0], Double.parseDouble(items[1]));
			context.write(obean, n);
		}
	}
	static class ItemPartition extends Partitioner<OrderBean, NullWritable>{
		
		@Override
		public int getPartition(OrderBean obean, NullWritable arg1, int numtasks) {
			return (obean.getOrder_id().hashCode()&Integer.MAX_VALUE)%numtasks;
		}
		
	}
	static class ItemGroupComprator extends WritableComparator{
		public ItemGroupComprator() {
			//不写true会报compareTo空指针异常
			super(OrderBean.class,true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			OrderBean abean = (OrderBean) a;
			OrderBean bbean = (OrderBean) b;
			return abean.getOrder_id().compareTo(bbean.getOrder_id());
		}
	}
	static class MaxOrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
		@Override
		protected void reduce(OrderBean keyBean, Iterable<NullWritable> v,Context context)
				throws IOException, InterruptedException {
			context.write(keyBean, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MaxOrder.class);
		job.setMapperClass(MaxOrderMapper.class);
		job.setReducerClass(MaxOrderReducer.class);
		job.setGroupingComparatorClass(ItemGroupComprator.class);
		job.setPartitionerClass(ItemPartition.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/order/input"));
		Path outpath = new Path("G:/hadoop_test/order/output");
		job.setNumReduceTasks(1);
		if(FileSystem.get(conf).exists(outpath)){
			FileSystem.get(conf).delete(outpath,true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
