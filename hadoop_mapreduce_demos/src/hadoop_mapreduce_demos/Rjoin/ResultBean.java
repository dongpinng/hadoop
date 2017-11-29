package hadoop_mapreduce_demos.Rjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ResultBean implements Writable{
	private String order_id;
	private String order_time;
	private String order_pro;
	private String order_amount;
	private String order_price;
	//用来区分表
	private int flag;
	public String getOrder_id() {
		return order_id;
	}
	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}
	public String getOrder_time() {
		return order_time;
	}
	public void setOrder_time(String order_time) {
		this.order_time = order_time;
	}
	public String getOrder_pro() {
		return order_pro;
	}
	public void setOrder_pro(String order_pro) {
		this.order_pro = order_pro;
	}
	public String getOrder_amount() {
		return order_amount;
	}
	public void setOrder_amount(String order_amount) {
		this.order_amount = order_amount;
	}
	public String getOrder_price() {
		return order_price;
	}
	public void setOrder_price(String order_price) {
		this.order_price = order_price;
	}
	public int getFlag() {
		return flag;
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.order_id = in.readUTF();
		this.order_time = in.readUTF();
		this.order_pro = in.readUTF();
		this.order_amount = in.readUTF();
		this.order_price = in.readUTF();
		this.flag = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeUTF(order_time);
		out.writeUTF(order_pro);
		out.writeUTF(order_amount);
		out.writeUTF(order_price);
		out.writeInt(flag);
	}
	@Override
	public String toString() {
		return order_id + "\t" + order_time + "\t" + order_pro
				+ "\t" + order_amount + "\t" + order_price;
	}
	public void set(String order_id, String order_time, String order_pro, String order_amount, String order_price,
			int flag) {
		this.order_id = order_id;
		this.order_time = order_time;
		this.order_pro = order_pro;
		this.order_amount = order_amount;
		this.order_price = order_price;
		this.flag = flag;
	}
	
	
}
