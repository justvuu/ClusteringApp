import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<LongWritable, Text, LongWritable, Customer> {

	private Customer[] currCentroids;
	private final LongWritable centroidId = new LongWritable();
	private final Customer customerInput = new Customer();

	@Override
	public void setup(Context context) {
		int nClusters = Integer.parseInt(context.getConfiguration().get("k"));
		this.currCentroids = new Customer[nClusters];
		for (int i = 0; i < nClusters; i++) {
			String[] centroid = context.getConfiguration().getStrings("C" + i);
			this.currCentroids[i] = new Customer(centroid);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] arrPropCustomer = value.toString().split(",");
		customerInput.set(arrPropCustomer);
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