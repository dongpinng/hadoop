package hadoop_mapreduce_demos.Rjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RJoin {
	static class RJoinMapper extends Mapper<LongWritable, Text, Text, ResultBean>{
		ResultBean rbean = new ResultBean();
		Text k = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] fields = new String(value.toString().getBytes(),"utf-8").split(",");
			FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
			String tab_name = fileInputSplit.getPath().getName();
			String pro_id="";
			if(tab_name.startsWith("order")){
				pro_id = fields[2];
				rbean.set(fields[0], fields[1], "", fields[3], "", 0);
			}else{
				pro_id = fields[0];
				rbean.set("","",fields[1],"",fields[2],1);
			}
			k.set(pro_id);
			context.write(k, rbean);
		}
	}
	
	static class RJoinReducer extends Reducer<Text, ResultBean, ResultBean, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<ResultBean> values,Context context)
				throws IOException, InterruptedException {
			ResultBean pro_bean = new ResultBean();
			List<ResultBean> rlists = new ArrayList<ResultBean>();
			for(ResultBean rb:values){
				if(rb.getFlag()==1){
					try {
						BeanUtils.copyProperties(pro_bean, rb);
					} catch (Exception e){
						e.printStackTrace();
					}
				}else{
					ResultBean order_bean = new ResultBean();
					try {
						BeanUtils.copyProperties(order_bean, rb);
						rlists.add(order_bean);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			for(ResultBean orBean : rlists){
				pro_bean.setOrder_id(orBean.getOrder_id());
				pro_bean.setOrder_time(orBean.getOrder_time());
				pro_bean.setOrder_amount(orBean.getOrder_amount());
				context.write(pro_bean, NullWritable.get());
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(RJoin.class);
		job.setMapperClass(RJoinMapper.class);
		job.setReducerClass(RJoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ResultBean.class);
		job.setOutputKeyClass(ResultBean.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/rjoin/input"));
		Path outpath = new Path("G:/hadoop_test/rjoin/output");
		if(FileSystem.get(conf).exists(outpath)){
			FileSystem.get(conf).delete(outpath,true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
