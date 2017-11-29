package hadoop_mapreduce_demos.mapjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 将表数据相对固定少的直接缓存到本地
 * Map端直接进行关联，这样避免数据倾斜。减少reduce压力
 * @author Win7
 *
 */
public class MapJoin {
	static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		Map<String,String> proMap = new HashMap<String,String>();
		Text k = new Text();
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pro.log"), "utf-8"));
			String line;
			while(StringUtils.isNotEmpty(line=br.readLine())){
				String[] fields = line.split(",");
				proMap.put(fields[0], fields[1]+"\t"+fields[2]);
			}
			br.close();
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			String line = fields[0]+"\t"+fields[1]+"\t"+proMap.get(fields[2])+"\t"+fields[3];
			k.set(line);
			context.write(k, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MapJoin.class);
		job.setMapperClass(MapJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path("G:/hadoop_test/rjoin/input"));
		Path outpath = new Path("G:/hadoop_test/rjoin/output_mapjoin");
		if (FileSystem.get(conf).exists(outpath)) {
			FileSystem.get(conf).delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		job.addCacheFile(new URI("file:/G:/hadoop_test/pro.log"));	
		
		//ָ缓存maptask的几种api
		/*job.addArchiveToClassPath(rachive);   将压缩包缓存到classpath中
		job.addCacheArchive(uri);			      将压缩包缓存到task目录下
		job.addCacheFile();					      将文件缓存在task目录下
		job.addFileToClassPath(); 			      将文件缓存在classpath中*/
		boolean rs = job.waitForCompletion(true);
		System.exit(rs?0:1);
	}
}
