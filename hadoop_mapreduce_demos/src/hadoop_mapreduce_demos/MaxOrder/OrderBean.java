package hadoop_mapreduce_demos.MaxOrder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {
	private String order_id;
	private double price;

	public String getOrder_id() {
		return order_id;
	}

	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@Override
	public String toString() {
		return order_id + "\t" + price;
	}

	public OrderBean(String order_id, double price) {
		set(order_id, price);
	}

	public OrderBean() {
	}

	public void set(String order_id, double price) {
		this.order_id = order_id;
		this.price = price;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.order_id = in.readUTF();
		this.price = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeDouble(price);
	}

	@Override
	public int compareTo(OrderBean o) {
		return (int) (o.getOrder_id().compareTo(this.order_id)==0?o.getPrice()-this.getPrice():-o.getOrder_id().compareTo(this.order_id));
	}

}
