package service;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.bson.Document;

public class KMapper extends Mapper<LongWritable, Document, LongWritable, Customer> {

	private Customer[] currCentroids;
	private final LongWritable centroidId = new LongWritable();
	private final Customer customerInput = new Customer();

	@Override
	public void setup(Context context) {
		System.out.println("Da vao setup cua mapper");
		int nClusters = Integer.parseInt(context.getConfiguration().get("k"));
		this.currCentroids = new Customer[nClusters];
		for (int i = 0; i < nClusters; i++) {
			String[] centroid = context.getConfiguration().getStrings("C" + i);
			this.currCentroids[i] = new Customer(centroid);
		}
	}

	@Override
	protected void map(LongWritable key, Document value, Context context) throws IOException, InterruptedException {
		System.out.println("Da vao setup cua mapper");
		int channel = value.getInteger("channel");
        int region = value.getInteger("region");
        int fresh = value.getInteger("fresh");
        int milk = value.getInteger("milk");
        int grocery = value.getInteger("grocery");
        int frozen = value.getInteger("frozen");
        int detergentsPaper = value.getInteger("detergents_paper");
        int delicassen = value.getInteger("delicassen");

        String result = String.format("%d,%d,%d,%d,%d,%d,%d,%d", channel, region, fresh, milk, grocery, frozen, detergentsPaper, delicassen);
		String[] arrPropCustomer = result.toString().split(",");
		customerInput.set(arrPropCustomer);
		System.out.println("Chuoi dau vao: " + result);
		double minDistance = Double.MAX_VALUE;
		int centroidIdNearest = 0;
		for (int i = 0; i < currCentroids.length; i++) {
			double distance = customerInput.calcDistance(currCentroids[i]);
			if (distance < minDistance) {
				centroidIdNearest = i;
				minDistance = distance;
			}
		}
		centroidId.set(centroidIdNearest);
		context.write(centroidId, customerInput);
	}
}