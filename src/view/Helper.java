package view;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;


public class Helper extends Configured implements Tool {
	private Configuration conf;
	
	public Helper() {
		this.conf = new Configuration();
		this.conf.set("fs.defaultFS", "hdfs://localhost:8020");
		this.conf.set("mapreduce.framework.name", "yarn");
		this.conf.set("mapreduce.jobtracker.address", "localhost:8021");
		this.conf.set("mapreduce.cluster.local.dir", "/tmp/hadoop-local");
	}
	
    public static String[] readCentroidsFromReducerOutput(Configuration conf, int kClusters, String folderOutputPath) throws IOException {
        String[] values = new String[kClusters];
        Path hdfsInputPath = new Path("/result/result.txt");

        try {
            FileSystem fs = hdfsInputPath.getFileSystem(conf);
            FSDataInputStream inputStream = fs.open(hdfsInputPath);

            // Đọc dữ liệu từ tệp và in ra màn hình
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
//                System.out.println(line);
            	values[count] = line;
            	count += 1;
            }
            reader.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
//        Path hdfsFolderPath = new Path("/k-output1"); // Thay đổi đường dẫn này thành thư mục bạn muốn liệt kê.
//
//        try {
//            FileSystem fs = hdfsFolderPath.getFileSystem(conf);
//            FileStatus[] status = fs.listStatus(hdfsFolderPath);
//
//            for (FileStatus fileStatus : status) {
//                if (fileStatus.isDirectory()) {
//                    System.out.println("Folder: " + fileStatus.getPath());
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        FileSystem hdfs = FileSystem.get(conf);
//        FileStatus[] status = hdfs.listStatus(new Path(folderOutputPath));
//
//        for (int i = 0; i < status.length; i++) {
//            if (!status[i].getPath().toString().endsWith("_SUCCESS")) {
//                Path outFilePath = status[i].getPath();
//                System.out.println("read " + outFilePath.toString());
//                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(outFilePath)));
//                String line = null;
//                int count = 0;
//                while ((line = br.readLine()) != null) {
//                    System.out.println(line);
//                    values[count] = line;
//                    count += 1;
//                }
//                br.close();
//            }
//        }

        return values;
    }

    public String[] start() throws Exception {
        String inputFilePath = "k-input/data-customer.csv";
        String outputFolderPath = "result";
        String outputFileName = "result.txt";

        if (inputFilePath == null || outputFolderPath == null) {
            System.err.printf(
                    "Usage: %s -Din <input file name> -Dlines <number of lines in input file> -Dout <Folder ouput> -Dresult <output file result> -Dk <number of clusters> -Dthresh <Threshold>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return null;
        }
        
//        Configuration conf = getConf();
//        Configuration conf = new Configuration();

        // Đặt các thuộc tính cấu hình tùy chỉnh
//        conf.set("fs.defaultFS", "hdfs://localhost:8020");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("mapreduce.jobtracker.address", "localhost:8021");
//        conf.set("mapreduce.cluster.local.dir", "/tmp/hadoop-local");
//        conf.addResource(new Path("/opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/hdfs-site.xml"));
//        conf.addResource(new Path("/opt/homebrew/Cellar/hadoop/3.3.6/libexec/etc/hadoop/core-site.xml"));
        String[] newCentroidPoints = readCentroidsFromReducerOutput(this.conf, 4, outputFolderPath);
        return newCentroidPoints;
    }
    
    
    public long WriteNewInput(String inputPath) throws IOException {
    	MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017"); // Thay đổi URL kết nối tùy theo cài đặt của bạn
    	MongoDatabase database = mongoClient.getDatabase("clustering"); // Thay đổi tên cơ sở dữ liệu của bạn
    	MongoCollection<Document> collection = database.getCollection("customers_test"); // Thay đổi tên bảng của bạn
    	MongoCursor<Document> cursor = collection.find().iterator();

    	FileSystem fs = FileSystem.get(this.conf);

        Path outputPath = new Path(inputPath);

        FSDataOutputStream outputStream = fs.create(outputPath);
    	long amount = collection.countDocuments();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            int channel = document.getInteger("channel");
            int region = document.getInteger("region");
            int fresh = document.getInteger("fresh");
            int milk = document.getInteger("milk");
            int grocery = document.getInteger("grocery");
            int frozen = document.getInteger("frozen");
            int detergentsPaper = document.getInteger("detergents_paper");
            int delicassen = document.getInteger("delicassen");

            // Tạo chuỗi định dạng và in ra kết quả
            String result = String.format("%d,%d,%d,%d,%d,%d,%d,%d", channel, region, fresh, milk, grocery, frozen, detergentsPaper, delicassen);
            System.out.println(result);
            outputStream.writeBytes(result + "\n");
        }
        outputStream.close();
        fs.close();
        return amount;
    }
    
    public void insertData(String value) {
    	MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017"); // Thay đổi URL kết nối tùy theo cài đặt của bạn
    	MongoDatabase database = mongoClient.getDatabase("clustering"); // Thay đổi tên cơ sở dữ liệu của bạn
    	MongoCollection<Document> collection = database.getCollection("customers_test"); // Thay đổi tên bảng của bạn
    	
    	String[] values = value.split(",");
    	Document document = new Document("channel", Integer.parseInt(values[0]))
                .append("region", Integer.parseInt(values[1]))
                .append("fresh", Integer.parseInt(values[2]))
                .append("milk", Integer.parseInt(values[3]))
                .append("grocery", Integer.parseInt(values[4]))
		    	.append("frozen", Integer.parseInt(values[5]))
		    	.append("detergents_paper", Integer.parseInt(values[6]))
		    	.append("delicassen", Integer.parseInt(values[7]));

            collection.insertOne(document);
            mongoClient.close();
    }
    
    public void ExecuteHadoop(String inputPath, String folderOutputPath, long lines) throws IOException {
    	
    	FileSystem fs = FileSystem.get(this.conf);
    	fs.delete(new Path(folderOutputPath), true);
    	try {
    	      String command = "/opt/homebrew/Cellar/hadoop/3.3.6/bin/hadoop jar ./src/view/Customer.jar " +
    	                       "-Din=" + inputPath + " " +
    	                       "-Dlines=" + lines + " " + 
    	                       "-Dresult=result.txt " +
    	                       "-Dmaxloop=100 " +
    	                       "-Dk=4 " +
    	                       "-Dthresh=0.0001 " +
    	                       "-DNumReduceTasks=2 " +
    	                       "-Dout=" + folderOutputPath;
//    	      command = "/opt/homebrew/Cellar/hadoop/3.3.6/bin/hadoop version";
    	      System.out.println(command);
//    	      ProcessBuilder processBuilder = new ProcessBuilder(command);
    	      Process process = Runtime.getRuntime().exec(command);
//              BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//              String line;
//
//              while ((line = reader.readLine()) != null) {
//                  System.out.println(line);
//              }
    	      InputStream errorStream = process.getErrorStream();
              BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream));
              String errorLine;
              while ((errorLine = errorReader.readLine()) != null) {
                  System.out.println(errorLine);
              }

              // Wait for the process to complete
              int exitCode = process.waitFor();
              if (exitCode == 0) {
                  System.out.println("Process completed successfully.");
              } else {
                  System.out.println("Process failed with exit code: " + exitCode);
              }
              
              
    	      
    	      
    	  	
    	  } catch (Exception ex) {
    	  	System.out.println("Loi: " + ex.toString());
    	  }
    }
    
    
    
    

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }
}