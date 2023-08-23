package view;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.border.EmptyBorder;

public class AppUI extends JPanel {
    private static final long serialVersionUID = 1L;
	private JTextField channelField, regionField, freshField, milkField, groceryField, frozenField, detergentsField, delicassenField;
    private JButton submitButton, trainButton;
    private JLabel resultLabel;
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
        Font boldFont = new Font(resultLabel.getFont().getName(), Font.BOLD, resultLabel.getFont().getSize());

        resultLabel.setFont(boldFont); 

        submitButton = new JButton("Submit");
        trainButton = new JButton("Train");
        
        trainButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                trainButton.setEnabled(false);
                SwingWorker<Void, Void> worker = new SwingWorker<Void, Void>() {
                    @Override
                    protected Void doInBackground() throws Exception {
                        try {
                            SwingUtilities.invokeLater(new Runnable() {
                                @Override
                                public void run() {
                                    resultLabel.setText("Processing...");
                                }
                            });

                            long amount = helper.WriteNewInput("/k-input/customer-data.txt");
                            System.out.println("Hadoop started !");
                            helper.ExecuteHadoop("/k-input/customer-data.txt", "/result", amount);
                            System.out.println("Hadoop finished !");
                        } catch (Exception e1) {
                            e1.printStackTrace();
                        } finally {
                            SwingUtilities.invokeLater(new Runnable() {
                                @Override
                                public void run() {
                                    resultLabel.setText("");
                                    trainButton.setEnabled(true);
                                }
                            });
                        }
                        return null;
                    }
                };
                worker.execute();
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
                String[] items = new String[3];
                try {
					items = helper.start();
				} catch (Exception e1) {
					e1.printStackTrace();
				}
                String newItem = channel + "," + region + "," + fresh + "," + milk + "," + grocery + "," + frozen + "," + detergents + "," + delicassen;
                Helper helper = new Helper();
                helper.insertData(newItem);
              int cluster = calcDistance(items, newItem);
                resultLabel.setText("Cluster: " + cluster);
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
        add(new JLabel());
        add(submitButton);
        add(trainButton);
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
            JFrame frame = new JFrame("Clustering App");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            AppUI appUI = new AppUI();
            frame.getContentPane().add(appUI);
            frame.pack();
            frame.setSize(500, 400);
            frame.setVisible(true);
        });
    }
}
