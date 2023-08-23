import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<LongWritable, Customer, LongWritable, Customer> {

	public void reduce(LongWritable centroidId, Iterable<Customer> customers, Context context)
			throws IOException, InterruptedException {

		String[] centroid = context.getConfiguration().getStrings("C" + centroidId);
		Customer currCentroid = new Customer(centroid);
		FileSystem hdfs = FileSystem.get(context.getConfiguration());
		FSDataOutputStream dos = hdfs.create(new Path("/k-output/cluster_" + (centroidId.get() + 1) + ".txt"), true);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));
		br.write("CENTROID: " + currCentroid.toString() + ":");
		br.newLine();
		Customer ptSum = Customer.copy(customers.iterator().next());
		br.write(ptSum.toString());
		br.newLine();
		
		while (customers.iterator().hasNext()) {
			Customer crr = customers.iterator().next();
			ptSum.sum(crr);
			br.write(crr.toString());
			br.newLine();
		}
		br.close();
		hdfs.close();

		context.write(centroidId, ptSum);
	}
}