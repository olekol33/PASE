package Inputs;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;


public class Probabilities {
    private int time;
    private int srcNode;
    private HashMap<Integer,Double> dstNodes;

    private static final int TIME=0;
    private static final int SRC=1;
    private static final int DST=2;
    private static final int PROBS=3;

    public static HashMap<Integer, double[][]> parseProb(HashMap<Integer, double[][]> probsKV, String probsPath, int numOfDCs) throws FileNotFoundException {

        ArrayList<String[]>  parsedCSV = InputHandler.parseCSV(probsPath, true);
        for (String[] prob : parsedCSV){
            int time = Integer.valueOf(prob[TIME]);
            double[][] P;
            if(!probsKV.containsKey(time)){ //if no data for this time slot
                P = new double[numOfDCs][numOfDCs];
                for (int i=0; i<numOfDCs;i++){
                    for (int j=0; j<numOfDCs;j++){
                        P[i][j] = 0;
                    }
                }
            }
            else
                P = probsKV.get(time);

            int srcNode = Integer.valueOf(prob[SRC]);
            String[] dsts = prob[DST].split("-");
            String[] probs  = prob[PROBS].split("-");
            if(dsts.length != probs.length)
                throw new IllegalArgumentException("Illegal prob vector sizes of srcNode " + prob[SRC]);
            for (int i=0; i<dsts.length; i++){
                Double nProb = Double.valueOf(probs[i]);
                if (nProb>1)
                    throw new IllegalArgumentException("Probability of this srcNode is > 1" + prob[SRC]);
                int dstNode = Integer.valueOf(dsts[i]);
                P[srcNode][dstNode] = nProb;
            }
            probsKV.put(time,P);
        }
        return probsKV;

    }

    public static HashMap<Integer, List<Double>> readLevyWalkProbs(){
        String filepath = "include/userProbsLW.csv";
        HashMap<Integer, List<Double>> probs = new HashMap<>();
        ArrayList<String[]> parsedCSV;
        parsedCSV = InputHandler.parseCSV(filepath, true);

        for (String[] row : parsedCSV) {
            int i=0;
            int user=-1;
            List<Double> userProbs = new ArrayList<>();
            for (String p : row){
                if(i==0){
                    user = Integer.parseInt(p);
                    i++;
                    continue;
                }
                userProbs.add(Double.valueOf(p));
                i++;
            }
            probs.put(user,userProbs);
        }
        return probs;
    }

    /**
     * Reads full location probabilities for the MAMI algorithm
     */
    public static double[][] readLocationProbsCSV(double mamiThreshold){
        String filepath = "include/intervalsInNodes.csv";

        ArrayList<String[]> parsedCSV;
        parsedCSV = InputHandler.parseCSV(filepath, false);
        int dcs = parsedCSV.size();
        double[][] probs = new double[dcs][dcs];
        int i=0;
        for (String[] row : parsedCSV) {
            int j=0;
            for (String p : row){
                if (Double.parseDouble(p) < mamiThreshold)
                    probs[i][j] = 0;
                else
                    probs[i][j] = Double.parseDouble(p);
                j++;
            }
            i++;
        }
        return probs;
    }

    /**
     * Generate HashMap of links using data center dataset
     * If link not within range (predefined value), bw and delay are 0
     */
    public static HashMap<Integer, double[][]> readProbsXlsx(boolean noMobilityModel, double pThreshold,
                                                             int numPredPerRequest, boolean mergeProbabilitiesFile){
        HashMap<Integer, double[][]> probs = new HashMap<>();
        try (FileInputStream  inp = new FileInputStream("include/probabilities.xlsx")) {
            XSSFWorkbook wb = new XSSFWorkbook(inp);
            for (int s=0 ;s < wb.getNumberOfSheets(); s++){
                XSSFSheet  sheet = wb.getSheetAt(s);
                int rowsCount = sheet.getPhysicalNumberOfRows();
                int colCounts = sheet.getRow(0).getLastCellNum();
                double[][] P = new double[rowsCount][colCounts];
                for (int i = 0; i < rowsCount; i++) {
                    Row row = sheet.getRow(i);
                    colCounts = sheet.getRow(i).getLastCellNum();
                    for (int j = 0; j < colCounts; j++) {
                        Cell cell = row.getCell(j);
                        if(noMobilityModel)
                            P[i][j] = 0;
                        else {
                            if (cell.getNumericCellValue() < pThreshold)
                                P[i][j] = 0;
                            else
                                P[i][j] = cell.getNumericCellValue();
                        }
                    }
                    if (!mergeProbabilitiesFile && !noMobilityModel) {
                        if (Arrays.stream(P[i]).sum() == 0) {
                            P[i][i] = Math.max(pThreshold + 0.01, 0.05);
                        }
                        P[i] = Probabilities.getKeysWithNLargestValues(P[i], numPredPerRequest);
                    }
                }
                probs.put(s,P);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (mergeProbabilitiesFile)
            probs = getMeanOf3dArray(probs, numPredPerRequest, pThreshold);
        return probs;
    }

    private static HashMap<Integer, double[][]> getMeanOf3dArray(HashMap<Integer, double[][]> old_matrix_hash,
                                                                 int numPredPerRequest, double pThreshold){
        HashMap<Integer, double[][]> new_matrix_hash = new HashMap<>();
        double[][] P = old_matrix_hash.get(0);
        int dDst = P[0].length;
        int dSrc = P.length;
        double[][] new_matrix = new double[dSrc][dDst];
        for (int i = 0; i< dSrc; i++){
            for (int j = 0; j<dDst; j++){
                for(int s=0; s< old_matrix_hash.size(); s++){
                    P = old_matrix_hash.get(s);
                    new_matrix[i][j] += P[i][j];
                }
                new_matrix[i][j] /= old_matrix_hash.size();
            }
        }
        for (int i=0 ; i< dSrc; i++) {
            for (int j=0 ; j < dDst ; j++){
                if (new_matrix[i][j] > 0 && new_matrix[i][j] < pThreshold)
                    new_matrix[i][j] = 0;
            }
            new_matrix[i] = Probabilities.getKeysWithNLargestValues(new_matrix[i], numPredPerRequest);
        }
        for(int s=0; s< old_matrix_hash.size(); s++)
            new_matrix_hash.put(s, new_matrix);
        return new_matrix_hash;
    }

    public static double[] getKeysWithNLargestValues(double[] vec, int n) {
        if (n==0)
            return vec;
        HashMap<Integer, Double> map = new HashMap<>();

        TreeMap<Double, Set<Integer>> valueToKeysMap = new TreeMap<>();
        for (int i=0; i< vec.length; i++)
            map.put(i, vec[i]);
        for (Map.Entry<Integer, Double> entry : map.entrySet()) {
            Double value = entry.getValue();
            Integer key = entry.getKey();
            Set<Integer> keys = valueToKeysMap.get(value);
            if (keys == null) {
                keys = new HashSet<>();
                valueToKeysMap.put(value, keys);
            }
            keys.add(key);
        }

        List<Integer> keysWithNLargestValues = new ArrayList<>();
        NavigableMap<Double, Set<Integer>> nLargestValues = valueToKeysMap.descendingMap();
        for (Double value : nLargestValues.keySet()) {
            for(int i : valueToKeysMap.get(value)){
                if (keysWithNLargestValues.size() >= n) {
                    return Probabilities.getArrayOfValues(keysWithNLargestValues, vec);
                }
                keysWithNLargestValues.add(i);
            }
        }
        return null;
    }

    private static double[] getArrayOfValues(List<Integer> keysWithNLargestValues , double[] vec){
        double[] newVec = new double[vec.length];
        for(int i =0; i< keysWithNLargestValues.size(); i++)
            newVec[keysWithNLargestValues.get(i)] = vec[keysWithNLargestValues.get(i)];
        return newVec;
    }


    public static HashMap<Integer, double[][]> readProbsFromXlsx(boolean noMobilityModel) throws FileNotFoundException {
        HashMap<Integer, double[][]> probs = new HashMap<>();
        try (FileInputStream  inp = new FileInputStream("include/probabilities2.xlsx")) {
            XSSFWorkbook wb = new XSSFWorkbook(inp);
            for (int s=0;s<wb.getNumberOfSheets(); s++){
                XSSFSheet  sheet = wb.getSheetAt(s);
                int rowsCount = sheet.getPhysicalNumberOfRows();
                int colCounts = sheet.getRow(0).getLastCellNum();
                double[][] P = new double[rowsCount][colCounts];
                for (int i = 0; i < rowsCount; i++) {
                    Row row = sheet.getRow(i);
                    colCounts = sheet.getRow(i).getLastCellNum();
                    for (int j = 0; j < colCounts; j++) {
                        Cell cell = row.getCell(j);
                        if(noMobilityModel)
                            P[i][j] = 0;
                        else
                            P[i][j] = cell.getNumericCellValue();
                    }
                }
                probs.put(s,P);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return probs;
    }
}
