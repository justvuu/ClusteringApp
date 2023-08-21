package service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.hadoop.MongoInputFormat;
import org.bson.BSONObject;

//import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class Service extends Configured implements Tool{

	public static Customer[] initRandomCentroids(int kClusters, int nLineOfInputFile, String inputFilePath,
			Configuration conf) throws IOException {
		Customer[] customers = new Customer[kClusters];

		List<Integer> lstLinePos = new ArrayList<Integer>();
		Random random = new Random();
		int pos;
		while (lstLinePos.size() < kClusters) {
			pos = random.nextInt(nLineOfInputFile);
			if (!lstLinePos.contains(pos)) {
				lstLinePos.add(pos);
			}
		}
		Collections.sort(lstLinePos);

		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(new Path(inputFilePath));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		int row = 0;
		int i = 0;
		while (i < lstLinePos.size()) {
			pos = lstLinePos.get(i);
			String customer = br.readLine();
			if (row == pos) {
				customers[i] = new Customer(customer.split(","));
				i++;
			}
			row++;
		}
		br.close();
		return customers;

	}

	public static void saveCentroidsForShared(Configuration conf, Customer[] customers) {
		for (int i = 0; i < customers.length; i++) {
			String centroidName = "C" + i;
			conf.unset(centroidName);
			conf.set(centroidName, customers[i].toString());
		}
	}

	public static Customer[] readCentroidsFromReducerOutput(Configuration conf, int kClusters,
			String folderOutputPath) throws IOException, FileNotFoundException {
		Customer[] customers = new Customer[kClusters];
		FileSystem hdfs = FileSystem.get(conf);
		FileStatus[] status = hdfs.listStatus(new Path(folderOutputPath));

		for (int i = 0; i < status.length; i++) {

			if (!status[i].getPath().toString().endsWith("_SUCCESS")) {
				Path outFilePath = status[i].getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(outFilePath)));
				String line = null;
				while ((line = br.readLine()) != null) {
					String[] strCentroidInfo = line.split("\t");
					int centroidId = Integer.parseInt(strCentroidInfo[0]);
					String[] attrPoint = strCentroidInfo[1].split(",");
					customers[centroidId] = new Customer(attrPoint);
				}
				br.close();
			}
		}

		hdfs.delete(new Path(folderOutputPath), true);

		return customers;
	}

	private static boolean checkStopKMean(Customer[] oldCentroids, Customer[] newCentroids, float threshold) {
		boolean needStop = true;
		for (int i = 0; i < oldCentroids.length; i++) {

			double dist = oldCentroids[i].calcDistance(newCentroids[i]);
			needStop = dist <= threshold;
			if (!needStop) {
				return false;
			}
		}
		return true;
	}

	private static void writeFinalResult(Configuration conf, Customer[] centroidsFound, String outputFilePath,
			Customer[] centroidsInit) throws IOException {
		FileSystem hdfs = FileSystem.get(conf);
		FSDataOutputStream dos = hdfs.create(new Path(outputFilePath), true);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

		for (int i = 0; i < centroidsFound.length; i++) {
			br.write(centroidsFound[i].toString());
			br.newLine();
		}

		br.close();
		hdfs.close();
	}

	public static Customer[] copyCentroids(Customer[] customers) {
		Customer[] savedCustomers = new Customer[customers.length];
		for (int i = 0; i < savedCustomers.length; i++) {
			savedCustomers[i] = Customer.copy(customers[i]);
		}
		return savedCustomers;
	}

	public static int MAX_LOOP = 50;

	public static void printCentroids(Customer[] customers, String name) {
		System.out.println("=> CURRENT CENTROIDS:");
		for (int i = 0; i < customers.length; i++)
			System.out.println(customers[i]);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.jobtracker.address", "localhost:8021");
		conf.set("mapreduce.cluster.local.dir", "/tmp/hadoop-local");

		int nClusters = conf.getInt("k", 4);
		String inputFilePath = conf.get("in", "/k-input2/customer-data2.txt");
		String outputFolderPath = conf.get("out", "/k-output9");
		String outputFileName = conf.get("result", "result.txt");
		float thresholdStop = conf.getFloat("thresh", 0.001f);
		int numLineOfInputFile = conf.getInt("lines", 30);
		int nReduceTask = conf.getInt("NumReduceTask", 2);
		MAX_LOOP = 50;
		
		Customer[] oldCentroidPoints = initRandomCentroids(nClusters, numLineOfInputFile, inputFilePath, conf);
		Customer[] centroidsInit = copyCentroids(oldCentroidPoints);
		saveCentroidsForShared(conf, oldCentroidPoints);
		int nLoop = 0;

		Customer[] newCentroidPoints = null;
		 String javaHome = System.getProperty("JAVA_HOME");

        // Print the JAVA_HOME value to the logs.
        System.out.println("JAVA_HOME on this node: " + javaHome);

//		System.setProperty("JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk-11.0.12.jdk/Contents/Home/bin/java");
		while (true) {
			System.out.println("Job started");
			nLoop++;
			if (nLoop == MAX_LOOP) {
				break;
			}
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "K-Mean");
			job.setJarByClass(Service.class);
			job.setMapperClass(KMapper.class);
			job.setCombinerClass(KCombiner.class);
			job.setReducerClass(KReducer.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Customer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
//			job.setInputFormatClass(MongoInputFormat.class);
//			Configuration mongodbConfig = new Configuration();
//			mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/clustering.customers");
//			job.getConfiguration().set("mongo.input.uri", mongodbConfig.get("mongo.input.uri"));
			
			FileInputFormat.addInputPath(job, new Path(inputFilePath));

			FileOutputFormat.setOutputPath(job, new Path(outputFolderPath));
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setNumReduceTasks(nReduceTask);

			boolean ret = job.waitForCompletion(true);
			if (!ret) {
				return -1;
			}

			newCentroidPoints = readCentroidsFromReducerOutput(conf, nClusters, outputFolderPath);
			boolean needStop = checkStopKMean(newCentroidPoints, oldCentroidPoints, thresholdStop);

			oldCentroidPoints = copyCentroids(newCentroidPoints);

			if (needStop) {
				break;
			} else {
				saveCentroidsForShared(conf, newCentroidPoints);
			}

		}
		if (newCentroidPoints != null) {
			printCentroids(newCentroidPoints, "");
			writeFinalResult(conf, newCentroidPoints, outputFolderPath + "/" + outputFileName, centroidsInit);
		}

		return 1;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Service(), args);
		System.out.println("FINISHED " + exitCode);
		System.exit(exitCode);
		
	}
}
