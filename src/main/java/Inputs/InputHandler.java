package Inputs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class InputHandler {

    public static ArrayList<String[]> parseCSV(String filepath, Boolean skipHeader){
        try {
            Scanner scanner = new Scanner(new File(filepath));
            String delimiter = ",";
            scanner.useDelimiter(delimiter);
            //skip header
            if (skipHeader)
                scanner.nextLine().split(delimiter);
            ArrayList<String[]> csvArr = new ArrayList<>();
            while (scanner.hasNextLine()) {
                String[] line = scanner.nextLine().split(delimiter);
                csvArr.add(line);
            }
            scanner.close();
            return csvArr;
        }
        catch (FileNotFoundException e){
            System.out.println();
            System.exit(1);
            return null;
        }
    }
}
