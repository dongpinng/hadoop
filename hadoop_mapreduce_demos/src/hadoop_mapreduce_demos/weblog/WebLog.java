package hadoop_mapreduce_demos.weblog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 自定义OutputFormat:
 * 增强解析:将URL匹配数据库中的数据，如果有，则写出新的内容；如果没有，则进行爬取
 * @author Win7
 *
 */
public class WebLog {
	static class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		Map<String,String> ruleMap = new HashMap<String,String>();
		Text k = new Text();
		NullWritable v = NullWritable.get();
		//从数据库中加载规则信息到ruleMap中
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			DBLoader.dbLoader(ruleMap);
		}
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//获取一个计数器来记录不合法的日志行数
			Counter counter = context.getCounter("malformed","malformedline");
			String line = value.toString();
			String[] fields = line.split(" ");
			if("400".equals(fields[6])||"/".equals(fields[6])||!fields[10].contains("http://")){
				counter.increment(1);
				return;
			}
			String url = fields[10].replaceAll("\"","");
			String content_tag = ruleMap.get(url);
			//判断内容标签是否为空，如果为空，则只输出url到待爬清单，如果有值，则输出到增强日志
			if(content_tag==null){
				k.set(url+"\t"+"tocrawl"+"\n");
				context.write(k, v);
			}else{
				k.set(line+"\t"+content_tag+"\n");
				context.write(k, v);
			}
		}
	}
	/**
	 * maptask或者reducetask在最终输出时，先调用OutputFormat的getRecoderWriter方法拿到一个RecordWriter然后在调用write方法
	 * @author Win7
	 *
	 */
	static class WebLogOutPutFormat extends FileOutputFormat<Text, NullWritable>{

		@Override
		public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path out = new Path("file:/G:/hadoop_test/data/output/out.log");
			Path tocraw = new Path("file:/G:/hadoop_test/data/outputtocrawl/url.log");
			FSDataOutputStream outfs = fs.create(out);
			FSDataOutputStream tocrawfs = fs.create(tocraw);
			return new WebLogRecordWriter(outfs,tocrawfs);
		}
	}
	/**
	 * 构造一个自己的recordWriter
	 * @author Win7
	 *
	 */
	static class WebLogRecordWriter extends RecordWriter<Text, NullWritable>{
		FSDataOutputStream outfs = null;
		FSDataOutputStream tocrawfs = null;
		
		public WebLogRecordWriter(FSDataOutputStream outfs, FSDataOutputStream tocrawfs) {
			this.outfs = outfs;
			this.tocrawfs = tocrawfs;
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			if(outfs!=null){
				outfs.close();
			}
			if(tocrawfs!=null){
				tocrawfs.close();
			}
		}

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			String rs = key.toString();
			//如要写的数据时待爬URL,则写入带爬清单文件
			if(rs.contains("tocrawl")){
				tocrawfs.write(rs.getBytes());
			}else{
				//如要写出是增强日志
				outfs.write(rs.getBytes());
			}
		}
		
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WebLog.class);
		job.setMapperClass(WebLogMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(WebLogOutPutFormat.class);
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/data/input"));
		FileOutputFormat.setOutputPath(job, new Path("G:/hadoop_test/data/output"));
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
