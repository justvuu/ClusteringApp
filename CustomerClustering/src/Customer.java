import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Customer implements Writable {

	private float channel;
	private float region;
	private float fresh;
	private float milk;
	private float grocery;
	private float frozen;
	private float detergents_paper;
	private float delicassen;
	
	private int nCustomers;

	public Customer() {

	}

	public Customer(final float[] c) {
		this.set(c);
	}

	public Customer(final String[] s) {
		this.set(s);
	}

	public static Customer copy(final Customer c) {
		Customer ret = new Customer();
		ret.channel = c.channel;
		ret.region = c.region;
		ret.fresh = c.fresh;
		ret.milk = c.milk;
		ret.grocery = c.grocery;
		ret.frozen = c.frozen;
		ret.detergents_paper = c.detergents_paper;
		ret.delicassen = c.delicassen;
		ret.nCustomers = c.nCustomers;
		return ret;
	}

	public void set(final float[] c) {
		this.nCustomers = 1;
		this.channel = c[0];
		this.region = c[1];
		this.fresh = c[2];
		this.milk = c[3];
		this.grocery = c[4];
		this.frozen = c[5];
		this.detergents_paper = c[6];
		this.delicassen = c[7];
	}

	public void set(final String[] s) {
		this.nCustomers = 1;
		this.channel = Float.parseFloat(s[0]);
		this.region = Float.parseFloat(s[1]);
		this.fresh = Float.parseFloat(s[2]);
		this.milk = Float.parseFloat(s[3]);
		this.grocery = Float.parseFloat(s[4]);
		this.frozen = Float.parseFloat(s[5]);
		this.detergents_paper = Float.parseFloat(s[6]);
		this.delicassen = Float.parseFloat(s[7]);
	}

	@Override
	public void readFields(final DataInput in) throws IOException {
		this.nCustomers = in.readInt();
		this.channel = in.readFloat();
		this.region = in.readFloat();
		this.fresh = in.readFloat();
		this.milk = in.readFloat();
		this.grocery = in.readFloat();
		this.frozen = in.readFloat();
		this.detergents_paper = in.readFloat();
		this.delicassen = in.readFloat();
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.nCustomers);
		
		out.writeFloat(this.channel);
		out.writeFloat(this.region);
		out.writeFloat(this.fresh);
		out.writeFloat(this.milk);
		out.writeFloat(this.grocery);
		out.writeFloat(this.frozen);
		out.writeFloat(this.detergents_paper);
		out.writeFloat(this.delicassen);
	}

	@Override
	public String toString() {
		StringBuilder customer = new StringBuilder();
		customer.append(Float.toString(this.channel));
		customer.append(",");
		customer.append(Float.toString(this.region));
		customer.append(",");
		customer.append(Float.toString(this.fresh));
		customer.append(",");
		customer.append(Float.toString(this.milk));
		customer.append(",");
		customer.append(Float.toString(this.grocery));
		customer.append(",");
		customer.append(Float.toString(this.frozen));
		customer.append(",");
		customer.append(Float.toString(this.detergents_paper));
		customer.append(",");
		customer.append(Float.toString(this.delicassen));
		return customer.toString();
	}

	public void sum(Customer c) {

		this.channel += c.channel;
		this.region += c.region;
		this.fresh += c.fresh;
		this.milk += c.milk;
		this.grocery += c.grocery;
		this.frozen += c.frozen;
		this.detergents_paper += c.detergents_paper;
		this.delicassen += c.delicassen;
		this.nCustomers += c.nCustomers;
	}

	public double calcDistance(Customer c) {

		double dist = 0.0;

	    float[] thisAttributes = new float[] {
	        this.channel, this.region, this.fresh, this.milk,
	        this.grocery, this.frozen, this.detergents_paper, this.delicassen
	    };

	    float[] cAttributes = new float[] {
	        c.channel, c.region, c.fresh, c.milk,
	        c.grocery, c.frozen, c.detergents_paper, c.delicassen
	    };

	    for (int i = 0; i < thisAttributes.length; i++) {
	        dist += Math.pow(Math.abs(thisAttributes[i] - cAttributes[i]), 2);
	    }

	    dist = Math.sqrt(dist);
	    return dist;

	}

	public void calcAverage() {
		this.channel = (float) Math.round((this.channel / this.nCustomers) * 100000) / 100000.0f;
		this.region = (float) Math.round((this.region / this.nCustomers) * 100000) / 100000.0f;
		this.fresh = (float) Math.round((this.fresh / this.nCustomers) * 100000) / 100000.0f;
		this.milk = (float) Math.round((this.milk / this.nCustomers) * 100000) / 100000.0f;
		this.grocery = (float) Math.round((this.grocery / this.nCustomers) * 100000) / 100000.0f;
		this.frozen = (float) Math.round((this.frozen / this.nCustomers) * 100000) / 100000.0f;
		this.detergents_paper = (float) Math.round((this.detergents_paper / this.nCustomers) * 100000) / 100000.0f;
		this.delicassen = (float) Math.round((this.delicassen / this.nCustomers) * 100000) / 100000.0f;
		
		
		this.nCustomers = 1;
	}
}