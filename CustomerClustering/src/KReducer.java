import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<LongWritable, Customer, Text, Text> {

	private final Text newCentroidId = new Text();
	private final Text newCentroidValue = new Text();

	public void reduce(LongWritable centroidId, Iterable<Customer> partialSums, Context context)
			throws IOException, InterruptedException {

		Customer ptFinalSum = Customer.copy(partialSums.iterator().next());
		while (partialSums.iterator().hasNext()) {
			Customer crr = partialSums.iterator().next();
			ptFinalSum.sum(crr);
		}
		
		ptFinalSum.calcAverage();

		newCentroidId.set(centroidId.toString());
		newCentroidValue.set(ptFinalSum.toString());
		context.write(newCentroidId, newCentroidValue);
	}
}