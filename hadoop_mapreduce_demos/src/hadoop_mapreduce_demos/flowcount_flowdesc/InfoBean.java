package hadoop_mapreduce_demos.flowcount_flowdesc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class InfoBean implements WritableComparable<InfoBean>{
	private LongWritable upflow;
	private LongWritable downflow;
	private Long sumflow;
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
		return  upflow + "\t" + downflow + "\t" +sumflow;
	}
	public InfoBean(LongWritable upflow, LongWritable downflow) {
		set(upflow,downflow);
	}
	public InfoBean() {
	}
	public void set(LongWritable upflow, LongWritable downflow){
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = upflow.get() + downflow.get();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upflow = new LongWritable(in.readLong());
		this.downflow = new LongWritable(in.readLong());
		this.sumflow = in.readLong();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upflow.get());
		out.writeLong(downflow.get());
		out.writeLong(sumflow);
	}
	@Override
	public int compareTo(InfoBean o) {
		return sumflow>(o.getDownflow().get()+o.getUpflow().get())?-1:1;
	}
}
