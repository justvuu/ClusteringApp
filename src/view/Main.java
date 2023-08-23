package view;
import javax.swing.*;

public class Main {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            createAndShowGUI();
        });
    }

    private static void createAndShowGUI() {
        JFrame frame = new JFrame("Clustering App");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        AppUI appUI = new AppUI();
        frame.getContentPane().add(appUI);

        frame.pack();
        frame.setVisible(true);
    }
}
