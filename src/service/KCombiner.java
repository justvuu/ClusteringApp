package service;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<LongWritable, Customer, LongWritable, Customer> {

	public void reduce(LongWritable centroidId, Iterable<Customer> customers, Context context)
			throws IOException, InterruptedException {

		String[] centroid = context.getConfiguration().getStrings("C" + centroidId);
		System.out.println("Da vao combiner");
		Customer currCentroid = new Customer(centroid);
//		System.out.println("---Current centroid: " + currCentroid.toString() + ":");
		
		Customer ptSum = Customer.copy(customers.iterator().next());
		
//		System.out.println("\t" + ptSum.toString());
		while (customers.iterator().hasNext()) {
			Customer crr = customers.iterator().next();
//			System.out.println("\t" + crr.toString());
			ptSum.sum(crr);
			
//			ptSum.sum(points.iterator().next());
		}
		
//		System.out.println("\n---TEST---");
//		System.out.println("-Output of KCombiner:\n");
//		System.out.println(ptSum.toString());
//		
//		System.out.print("\n");

		context.write(centroidId, ptSum);
	}
}