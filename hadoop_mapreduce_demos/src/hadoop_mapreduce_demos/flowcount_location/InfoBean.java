package hadoop_mapreduce_demos.flowcount_location;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class InfoBean implements Writable{
	private LongWritable upflow;
	private LongWritable downflow;
	public LongWritable getUpflow() {
		return upflow;
	}
	public void setUpflow(LongWritable upflow) {
		this.upflow = upflow;
	}
	public LongWritable getDownflow() {
		return downflow;
	}
	public void setDownflow(LongWritable downflow) {
		this.downflow = downflow;
	}
	@Override
	public String toString() {
		return  upflow + "\t" + downflow;
	}
	public InfoBean(LongWritable upflow, LongWritable downflow) {
		set(upflow,downflow);
	}
	public InfoBean() {
	}
	public void set(LongWritable upflow, LongWritable downflow){
		this.upflow = upflow;
		this.downflow = downflow;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upflow = new LongWritable(in.readLong());
		this.downflow = new LongWritable(in.readLong());
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upflow.get());
		out.writeLong(downflow.get());
	}
}
