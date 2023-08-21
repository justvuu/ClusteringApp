package service;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<LongWritable, Customer, Text, Text> {

	private final Text newCentroidId = new Text();
	private final Text newCentroidValue = new Text();

	public void reduce(LongWritable centroidId, Iterable<Customer> partialSums, Context context)
			throws IOException, InterruptedException {
		System.out.println("Da vao reducer");

		Customer ptFinalSum = Customer.copy(partialSums.iterator().next());
//		System.out.println("\n---TEST---");
		while (partialSums.iterator().hasNext()) {
//			System.out.println("Vong lap");
			Customer crr = partialSums.iterator().next();
			ptFinalSum.sum(crr);
//			System.out.println(crr.toString());
		}
		
		ptFinalSum.calcAverage();
//		System.out.println("--Output of KReducer");
//		System.out.println("Tam Id = " + centroidId.toString());
//		System.out.println(ptFinalSum.toString());

		newCentroidId.set(centroidId.toString());
		newCentroidValue.set(ptFinalSum.toString());
		context.write(newCentroidId, newCentroidValue);
	}
}