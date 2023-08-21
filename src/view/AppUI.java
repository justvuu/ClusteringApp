package view;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.io.IOUtils;
import service.Service;

import javax.swing.border.EmptyBorder;

public class AppUI extends JPanel {
    private JTextField channelField, regionField, freshField, milkField, groceryField, frozenField, detergentsField, delicassenField;
    private JButton submitButton, trainButton;
    private JLabel resultLabel, progressLabel;
    private String[] lastFourLines;
    private Helper helper;

    public AppUI() {
        setLayout(new GridLayout(10, 2));
        setBorder(new EmptyBorder(10, 20, 10, 20)); 

        helper = new Helper();
        channelField = new JTextField();
        regionField = new JTextField();
        freshField = new JTextField();
        milkField = new JTextField();
        groceryField = new JTextField();
        frozenField = new JTextField();
        detergentsField = new JTextField();
        delicassenField = new JTextField();
        resultLabel = new JLabel("");
        progressLabel = new JLabel("");
        Font boldFont = new Font(resultLabel.getFont().getName(), Font.BOLD, resultLabel.getFont().getSize());

        resultLabel.setFont(boldFont); 

        submitButton = new JButton("Submit");
        trainButton = new JButton("Train");
//        try {
//            String command = "/opt/homebrew/Cellar/hadoop/3.3.6/bin/hadoop jar /Users/justvu/Desktop/Customer.jar " +
//                             "-Din=/k-input2/customer-data2.txt " +
//                             "-Dlines=30 " +
//                             "-Dresult=result.txt " +
//                             "-Dmaxloop=100 " +
//                             "-Dk=4 " +
//                             "-Dthresh=0.0001 " +
//                             "-DNumReduceTasks=2 " +
//                             "-Dout=/k-output7";
//            Process process = Runtime.getRuntime().exec(command);
//            process.waitFor();
//        	BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//            String line;
//            lastFourLines = new String[4];
//            int index = 0;
//
//            while ((line = reader.readLine()) != null) {
//                if (index >= 4) {
//                    // Shift previous lines up
//                    System.arraycopy(lastFourLines, 1, lastFourLines, 0, 3);
//                    lastFourLines[3] = line;
//                } else {
//                    lastFourLines[index] = line;
//                    index++;
//                }
//            }
//
//            reader.close();
//        	
//        } catch (Exception ex) {
//        	System.out.println("Loi: " + ex.toString());
//        }
        
        trainButton.addActionListener(new ActionListener() {
        	@Override
            public void actionPerformed(ActionEvent e) {
        		
//        		try {
//					helper.WriteNewInput();
//				} catch (IOException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
//        		Service service = new Service();
        		int ans = 0;
//        		System.setProperty("JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk-11.0.12.jdk/Contents/Home/bin/java");
        		try {
        			progressLabel.setText("Đang xử lý...");
//					Service.main(new String[]{});
        			Helper helper = new Helper();
        			long amount = helper.WriteNewInput("/k-input/customer-data.txt");
        			System.out.println("Hadoop started !");
        			helper.ExecuteHadoop("/k-input/customer-data.txt", "/result", amount);
        			System.out.println("Hadoop finished !");
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}finally {
		            // Clear the progress message, whether an exception occurred or not
		            progressLabel.setText("");
		        }
                System.out.println("clicked");
            }
        });
        
        submitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String channel = channelField.getText();
                String region = regionField.getText();
                String fresh = freshField.getText();
                String milk = milkField.getText();
                String grocery = groceryField.getText();
                String frozen = frozenField.getText();
                String detergents = detergentsField.getText();
                String delicassen = delicassenField.getText();
//                Helper helper = new Helper();
                String[] items = new String[4];
                try {
					items = helper.start();
//					for (String item : items) {
//                      System.out.println(item);
//                  }
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
                String newItem = channel + "," + region + "," + fresh + "," + milk + "," + grocery + "," + frozen + "," + detergents + "," + delicassen;
                Helper helper = new Helper();
                helper.insertData(newItem);
              int distance = calcDistance(items, newItem);
                resultLabel.setText("Cluster: " + distance);
            }
        });

        add(new JLabel("Channel:"));
        add(channelField);
        add(new JLabel("Region:"));
        add(regionField);
        add(new JLabel("Fresh:"));
        add(freshField);
        add(new JLabel("Milk:"));
        add(milkField);
        add(new JLabel("Grocery:"));
        add(groceryField);
        add(new JLabel("Frozen:"));
        add(frozenField);
        add(new JLabel("Detergents_Paper:"));
        add(detergentsField);
        add(new JLabel("Delicassen:"));
        add(delicassenField);
        add(resultLabel); 
        
        add(submitButton);
        add(trainButton);
        add(progressLabel);
    }
    
    public void HandleTraining() {
      try {
      String command = "/opt/homebrew/Cellar/hadoop/3.3.6/bin/hadoop jar /Users/justvu/Desktop/Customer.jar " +
                       "-Din=/k-input2/customer-data2.txt " +
                       "-Dlines=30 " +
                       "-Dresult=result.txt " +
                       "-Dmaxloop=100 " +
                       "-Dk=4 " +
                       "-Dthresh=0.0001 " +
                       "-DNumReduceTasks=2 " +
                       "-Dout=/k-output6";
      Process process = Runtime.getRuntime().exec(command);
      process.waitFor();
  	BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      lastFourLines = new String[4];
      int index = 0;

      while ((line = reader.readLine()) != null) {
          if (index >= 4) {
              // Shift previous lines up
              System.arraycopy(lastFourLines, 1, lastFourLines, 0, 3);
              lastFourLines[3] = line;
          } else {
              lastFourLines[index] = line;
              index++;
          }
      }

      reader.close();

      StringBuilder output = new StringBuilder();
      for (String lastLine : lastFourLines) {
          if (lastLine != null) {
              output.append(lastLine).append("\n");
          }
      }

      resultLabel.setText(output.toString());
  	
  } catch (Exception ex) {
  	System.out.println("Loi: " + ex.toString());
  }
    }
    
    public int calcDistance(String[] centroids, String newItem) {
    	double ans = Float.MAX_VALUE;
    	String[] value = newItem.split(",");
    	float[] thisAttributes = new float[] {
    	        Float.parseFloat(value[0]), Float.parseFloat(value[1]),Float.parseFloat(value[2]),Float.parseFloat(value[3]),
    	        Float.parseFloat(value[4]),Float.parseFloat(value[5]),Float.parseFloat(value[6]),Float.parseFloat(value[7])
    	    };
    	int cluster = 0;
    	for(String centroid : centroids) {
    		double dist =  0.0;
    		String[] selected = centroid.split(",");
        	float[] cAttributes = new float[] {
        	        Float.parseFloat(selected[0]), Float.parseFloat(selected[1]),Float.parseFloat(selected[2]),Float.parseFloat(selected[3]),
        	        Float.parseFloat(selected[4]),Float.parseFloat(selected[5]),Float.parseFloat(selected[6]),Float.parseFloat(selected[7])
        	    };

    	    for (int i = 0; i < thisAttributes.length; i++) {
    	        dist += Math.pow(Math.abs(thisAttributes[i] - cAttributes[i]), 2);
    	    }

    	    dist = Math.sqrt(dist);
    	    if(dist < ans) {
    	    	ans = dist;
    	    	cluster+=1;
    	    }
    	}
    	return cluster;
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("Ứng dụng Java Swing");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            AppUI appUI = new AppUI();
            frame.getContentPane().add(appUI);
            frame.pack();
            frame.setSize(500, 400);
            frame.setVisible(true);
        });
    }
}
