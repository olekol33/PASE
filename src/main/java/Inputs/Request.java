package Inputs;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.javatuples.Triplet;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class Request {
    private final int time;
    private int id;
    private int location;
    private double weight;
    private final int revenue;
    private boolean used;

    private double weightedRevenue;

    public double getTotalWeight() {
        return totalWeight;
    }

    public void setTotalWeight(double totalWeight) {
        this.totalWeight = totalWeight;
    }

    private double totalWeight;

    public double getWeight() {
        return weight;
    }

    public double getWeightedRevenue() {
        return weightedRevenue;
    }

    public void setWeight(double weight) {

        this.weight = weight;
        weightedRevenue = weight * revenue;
    }



    public void setId(int id) {
        this.id = id;
    }

    public void setLocation(int location) {
        this.location = location;
    }

    private String appS;
    private App app;

    public App getApp() {
        return app;
    }

    public void setApp(App app) {
        this.app = app;
    }

    public static ArrayList<List<Request>> genUserRequestsFromFile(HashMap<String, Object> config, ArrayList<Integer> apps){
        final int TIME=0;
        final int ID=1;
        final int LOCATION=2;
        int seed = (Integer)config.get("seed");
        boolean levyWalk = (Boolean)config.get("levyWalk");
        RandomGenerator rand = new Well19937c((seed));
        int numOfApps = apps.size();


        int reqID = 0;
        HashMap<Integer, Triplet<Integer,Integer,Integer>> reqIdHash = new HashMap<>(); //k: real id, v: <id,revenue,appID>

        ArrayList<List<Request>> requests = new ArrayList<>();
        String reqFilename;
        if (levyWalk)
            reqFilename = "include/requestsLW.xlsx";
        else
            reqFilename = "include/requests.xlsx";
        try (FileInputStream inp = new FileInputStream(reqFilename)) {
            XSSFWorkbook wb = new XSSFWorkbook(inp);
            int revenueStd = (int)config.get("revenueStd");
            int revenueMean = (int)config.get("revenueMean");
            boolean constantRevenue = false;
            if (revenueStd<=0) {
                revenueStd = 1;
                constantRevenue = true;
            }
            NormalDistribution normalRevenue = new NormalDistribution(rand,revenueMean,revenueStd);
            for (int s=0;s<wb.getNumberOfSheets(); s++){
                XSSFSheet sheet = wb.getSheetAt(s);
                int rowsCount = sheet.getPhysicalNumberOfRows();
                List<Request> intervalRequests = new ArrayList<>();
                for (int i = 0; i < rowsCount; i++) {
                    Row row = sheet.getRow(i);
                    int id;
                    int originalID = (int)row.getCell(ID).getNumericCellValue();
                    int revenue;
                    int selectedApp;
                    if(reqIdHash.containsKey(originalID)) {
                        id = reqIdHash.get(originalID).getValue0();
                        revenue = reqIdHash.get(originalID).getValue1();
                        selectedApp = reqIdHash.get(originalID).getValue2();

                    }
                    else {
                        if (constantRevenue)
                            revenue = revenueMean;
                        else {
                            revenue = (int) normalRevenue.sample();
                            while (revenue <= 0)
                                revenue = (int) normalRevenue.sample();
                        }
                        selectedApp = apps.get(App.sampleZipf(rand,numOfApps-1));
                        reqIdHash.put(originalID, Triplet.with(reqID,revenue,selectedApp));
                        id = reqID;
                        reqID++;

                    }
                    int t = (int)row.getCell(TIME).getNumericCellValue();

                    intervalRequests.add(new Request(t,id,
                            (int)row.getCell(LOCATION).getNumericCellValue(),
                            String.valueOf(selectedApp),revenue));
                }
                requests.add(s,intervalRequests);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        Properties properties = new Properties();

        for (Map.Entry<Integer,Triplet<Integer,Integer,Integer>> entry : reqIdHash.entrySet()) {
            properties.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue().getValue0()));
        }
        try {
            properties.store(new FileOutputStream("include/Req2GenericReqIdMap.txt"), "Original Request = Hashed Request");
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return requests;
    }

    public int getUsed() {
        if (used)
            return 1;
        else
            return 0;
    }

    public void setUnused() {
        this.used = false;
    }

    public int getId() {
        return id;
    }

    public int getLocation() {
        return location;
    }

    public String getAppS() {
        return appS;
    }
    public int getTime() {
        return time;
    }

    public int getRevenue() {
        return revenue;
    }

    public Request(int time, int id, int location, String app, int revenue) {
        this.time = time;
        this.id = id;
        this.location = location;
        this.appS = app;
        this.revenue = revenue;
        this.used = true;
    }
}
