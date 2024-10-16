import Inputs.*;
import ilog.concert.IloException;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.javatuples.Pair;

import java.io.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
public class OptimizationLauncher{
    double acceptableGap;
    double mamiThreshold;
    double algThreshold;
    boolean levyWalk;
    boolean end2endLatency;
    boolean onlineMode;
    boolean locationErrorModel;
    boolean applyLowRevenueHeuristic;
    boolean dynamicStateCost;
    double reqTh;
    HashMap<String,Object> config;
    List<String> expTypes;
    int dynamicStateCostStep;
    FileHandler fileHandler = null;

    int t;

    PrintWriter lpWriter;
    PrintWriter runtimeWriter;

    HashMap<Integer, Function> functions = new HashMap<>();
    HashMap<Integer, DC> dcs = new HashMap<>();
    HashMap<Integer, App> apps = new HashMap<>();
    HashMap<Pair<Integer,Integer>, Link> links = new HashMap<>();
    ArrayList<List<Request>> requests = new ArrayList<>();
    String expType;

    HashMap<Integer, DC> prevDCs = null;
    double[][] probMapMAMI = null;
    HashMap<Integer, double[][]> probs = null;
    HashMap<Integer, List<Double>> probsLW = null;
    double estCapRatio = 0;
    double actCapRatio = 0;

    double estMaxLimit = 0;
    double actMaxLimit = 0;
    int prevEstReqs = 0;
    int prevActReqs = 0;
    public OptimizationLauncher(HashMap<String, Object> config) throws Exception {
        this.config = config;
        getConfigurations();
        setExpTypes();
        getInputs();
        createRundir();
        executeRun();

    }

    private void getConfigurations() throws Exception {
        if(config==null)
            config = new ConfigParser().getConfig();
        acceptableGap = (Double) config.get("heuristicGap");
        mamiThreshold = (Double) config.get("mamiThreshold");
        algThreshold = (Double) config.get("algThreshold");
        levyWalk = (Boolean)config.get("levyWalk");
        end2endLatency = (Boolean)config.get("end2endLatency");
        locationErrorModel = (Boolean)config.get("locationErrorModel");
        applyLowRevenueHeuristic = (Boolean)config.get("applyLowRevenueHeuristic");
        dynamicStateCost = (Boolean)config.get("dynamicStateCost");
        reqTh = (Double) config.get("threshold");
        prevEstReqs = (int)config.get("initialRealisticCapacity") / 2;
        prevActReqs = prevEstReqs;
        estMaxLimit = (int)config.get("initialRealisticCapacity");
        actMaxLimit = estMaxLimit / 4;
    }

    private void setExpTypes(){
        expTypes = new ArrayList<>();

        if((Boolean)config.get("regularRun"))
            expTypes.add("regular");
        if((Boolean)config.get("fullRun"))
            expTypes.add("full");
        if((Boolean)config.get("onlineRun"))
            expTypes.add("online");
        if((Boolean)config.get("locationError"))
            expTypes.add("location_error");
        if((Boolean)config.get("onlineFullRun"))
            expTypes.add("online_full");
        if((Boolean)config.get("adversarialRun"))
            expTypes.add("adversarial");
        if((Boolean)config.get("mamiRun"))
            expTypes.add("mami");
    }

    private void getInputs(){
        if (dynamicStateCost) {
            int dynamicStateCostStepPercent = (int)config.get("dynamicStateCostStepPercent");
            int stateCost = (int)config.get("stateCost");
            if (dynamicStateCostStepPercent==0)
                dynamicStateCostStep = 0;
            else
                dynamicStateCostStep = stateCost / dynamicStateCostStepPercent;
            config.put("dynamicStateCostStep",dynamicStateCostStep);
        }

        if(onlineMode && locationErrorModel)
            throw new IllegalStateException("Cannot run more than 1 special model");
        if(levyWalk)
            probsLW = Probabilities.readLevyWalkProbs();
        else {
            int numPredPerRequest = (int)config.get("numPredPerRequest");
            boolean mergeProbabilitiesFile = (boolean)config.get("mergeProbabilitiesFile");
            if (numPredPerRequest>0)
                System.out.println("Limit number of prediction to " + numPredPerRequest + " per user");
            probs = Probabilities.readProbsXlsx(locationErrorModel, algThreshold , numPredPerRequest, mergeProbabilitiesFile);
        }

        if((Boolean)config.get("syntheticFunctions")) {
            functions = Function.genFunctions(config);
            if((Boolean)config.get("syntheticApps"))
                apps = App.genApps(config,functions);
             parseInputFiles(config,functions,dcs,apps,links);
        }
        else{
            parseInputFiles(config,functions,dcs,apps,links);
            if((Boolean)config.get("syntheticApps"))
                apps = App.genApps(config,functions);
        }
        //Synthetic generation of requests
        if((Boolean)config.get("syntheticRequests")) {
            ArrayList<Integer> dcList = new ArrayList<Integer>(dcs.keySet());
        }
        ArrayList<Integer> appList = new ArrayList<Integer>(apps.keySet());

        requests = Request.genUserRequestsFromFile(config,appList);
        processInputRequests(requests, apps);
        setDcCapacityByUtilization();
    }

    private void setDcCapacityByUtilization(){
        double dcUtilization = (Double) config.get("dcUtilization");
        if (dcUtilization==0)
            return;
        double totalDemand = getMeanDemandPerInterval();
        int numOfDCs = dcs.size();
        int dcCapacity = (int) ((totalDemand/dcUtilization) / numOfDCs);
        for(int d=0; d<dcs.size(); d++) {
            dcs.get(d).setCapacity(dcCapacity);
            dcs.get(d).setOriginalCapacity(dcCapacity);
        }
        System.out.println("Setting " + dcUtilization + " DC utilization -> " + dcCapacity);
    }

    private double getMeanDemandPerInterval(){
        int totalDemand = 0;
        for(int interval=0; interval < requests.size(); interval++){
            totalDemand += getIntervalDemand(interval);
        }
        return (double)totalDemand / requests.size();

    }

    private int getIntervalDemand(int interval){
        int totalDemand = 0;
        for(Request request : requests.get(interval))
            totalDemand += request.getApp().getTotalSize();
        return totalDemand;
    }


    private static void parseInputFiles(HashMap<String,Object> config,HashMap<Integer, Function> functions,
                                        HashMap<Integer, DC> dcs,
                                        HashMap<Integer,App> apps, HashMap<Pair<Integer,Integer>, Link> links
    ){
        String functionsPath = "include/functions.csv";
        String dcPath = "include/datacenters.csv";
        String appPath = "include/applications.csv";
        int allowedRange = (int) config.get("allowedRange");

        ArrayList<String[]> parsedCSV;

        if(functions.size()==0) {
            parsedCSV = InputHandler.parseCSV(functionsPath, true);
            for (String[] function : parsedCSV) {
                Function func = new Function(function);
                functions.put(func.getId(), func);
            }
        }
        parsedCSV = InputHandler.parseCSV(dcPath, true);
        for (String[] dc : parsedCSV){
            DC dc1 = new DC(dc,(int) config.get("stateCost"),(int) config.get("capacity"));
            dcs.put(dc1.getId(),dc1);
        }
        for(int d1=0;d1<dcs.size();d1++){
            List<Integer> neighbors = new ArrayList<>();
            DC dc1 = dcs.get(d1);
            for(int d2=0;d2<dcs.size();d2++){
                if(d1==d2) {
                    neighbors.add(d1);
                    continue;
                }
                DC dc2 = dcs.get(d2);
                if(DC.areDCsInRange(dc1,dc2,allowedRange))
                    neighbors.add(d2);
            }
            dc1.setNeigbors(neighbors);
        }

        Link.genLinks(links,config,dcs, (int) config.get("delay"),(int) config.get("bw") );

        if(apps.size()==0) {
            parsedCSV = InputHandler.parseCSV(appPath, true);
            for (String[] app : parsedCSV) {
                App app1 = new App(apps, functions, app);
                apps.put(app1.getId(), app1);
            }

            //add zero functions to mark location of user
            for(Map.Entry<Integer, App> kv : apps.entrySet()){
                App app = kv.getValue();
                app.addZeroFunctions(null, null);
            }
        }
    }

    public static ArrayList<List<Request>> processInputRequests(ArrayList<List<Request>> requests, HashMap<Integer,App> apps){
        for (List<Request> requestSet : requests){
            for (Request request : requestSet){
                //Get App variable from request (convert string to int)
                App appType = apps.get(Integer.valueOf(request.getAppS()));
                request.setApp(appType);
            }
        }
        return requests;
    }

    private void executeRun(){
        for(String type : expTypes) {
            expType = type;
            fileHandler = new FileHandler(config, expType);
            if (expType.equals("mami")) {
                mamiRun();
                continue;
            }
            try {
                writeLpSummary(expType);
                runPASE();
            }
            catch (IOException | IloException e){
                e.printStackTrace();
            }
            System.out.println("Run completed");
        }
    }

    private void runPASE() throws IOException, IloException {
        double pThreshold=0;
        System.gc();
        if(expType.contains("online")) {
            onlineMode = true;
            config.put("locationErrorModel",false);
        }
        else if(expType.contains("full")) {
            config.put("limitUserReqs", false);
            pThreshold = algThreshold;
        }
        int duration = (int)config.get("duration");
        Model model= null;
        String logdir = String.valueOf(fileHandler.getLogdir());
        Path dump = fileHandler.getDumpdir();
        List<Integer> reqIDs = new ArrayList<>();
        estCapRatio = (double) config.get("capacityRatioThreshold") / 2;
        actCapRatio = estCapRatio;
        for (t = 0; t < duration; t++)
            model = runPaseIteration(model, dump, logdir, reqIDs, pThreshold);
    }

    private Model runPaseIteration(Model model, Path dump, String logdir,
                                  List<Integer> reqIDs, double pThreshold) throws IOException, IloException {
        int duration = (int)config.get("duration");
        if (t == 0 && onlineMode) //skip first interval
            return null;

        if (t > 0 & !onlineMode) {
            runAdjust(model, dump, logdir, reqIDs);
            if (t + 1 == duration) //do not estimate further
                return null;
        }
        HashMap<Integer, DC> dcsCP = cloneDCs(prevDCs);
        HashMap<Pair<Integer, Integer>, Link> linksCP = cloneLinks();

        List<Request> estRequests;
        if (onlineMode) //t is increment and ran as actual in this loop
            estRequests = genWeightedRequests(requests.get(t),null, null, null, dcs, config, false, 0);
        else if (locationErrorModel)
            estRequests = genWeightedRequests(requests.get(t),requests.get(t+1), null, null, dcs,config, true, 0);
        else {
            if(levyWalk)
                estRequests = genWeightedRequests(requests.get(t), null, null, probsLW, dcs, config, true, pThreshold);
            else
                estRequests = genWeightedRequests(requests.get(t), null, probs.get(t), null, dcs, config, true, pThreshold);
        }
        requests.get(t).forEach(request -> reqIDs.add(request.getId()));
        List<Request> origReqs = new ArrayList<>(requests.get(t));
        printRunElements(dump, dcs, apps, functions, estRequests, origReqs);
        if (onlineMode) {
            List<Request> origRequests;
            if(logdir.contains("full"))
                origRequests = requests.get(t);
            else {
                Pair<List<Request>,Double> retPair = removeLowRevenueRequests(config, dcs, estRequests, requests.get(t),
                        actCapRatio, actMaxLimit, reqTh,prevActReqs , "Actual");
                origRequests = retPair.getValue0();
                actCapRatio = retPair.getValue1();
                prevActReqs=estRequests.size();
            }
            if(t>2)
                model.cleanup();
            model = null;
            model = new Model(dcsCP, apps, functions,
                    linksCP, estRequests, origRequests, reqIDs, config, t, false, expType);
            lpWriter.println("\n*** Online at interval " + t + " ***\n");
            System.out.println("\n*** Online at interval " + t + " ***\n");
            model.solve(false);
            prevDCs = model.getDcs();
            reqIDs.clear();
        }
        else {
            List<Request> origRequests;
            if(logdir.contains("full") || logdir.contains("location_error")) {
                origRequests = requests.get(t);
            }
            else {
                if (applyLowRevenueHeuristic) {
                    Pair<List<Request>,Double> retPair  = removeLowRevenueRequests(config, dcs, estRequests, requests.get(t),
                            estCapRatio, estMaxLimit, reqTh, prevEstReqs, "Preallocate");
                    origRequests = retPair.getValue0();
                    estCapRatio = retPair.getValue1();
                    prevEstReqs=estRequests.size();
                }
                else
                    origRequests = requests.get(t);
            }
            model = new Model(dcsCP, apps, functions,
                    linksCP, estRequests, origRequests, reqIDs, config, t + 1, true, expType);
            lpWriter.println("\n*** Estimate at interval " + t + " ***\n");
            System.out.println("\n*** Estimate at interval " + t + " ***\n");
            model.solve(true);
        }
        model.getSolution();
        model.analyzeUtilizedCapacities();
        if(onlineMode)
            model.updateDCStateCosts();
        runtimeWriter.println(model.getTime() + "," + model.getType() + "," +
                (int) model.getDetRunTime() +"," + model.getCplexRunTime() +
                "," + estCapRatio + "," + model.getOrigRequests().size());

        //Adjust capacity ratio by cplex solver gap from optimal
        if(!logdir.contains("full")){
            System.out.println();
            if(onlineMode){
                double gap = model.getGap();
                double[] pair = updateGap(gap,acceptableGap,actCapRatio, actMaxLimit, model);
                actCapRatio = pair[0];
                actMaxLimit = pair[1];
            }
            else{
                double gap = model.getGap();
                double[] pair = updateGap(gap,acceptableGap,estCapRatio, estMaxLimit, model);
                estCapRatio = pair[0];
                estMaxLimit = pair[1];
            }
        }
        if (t + 1 == duration)
            model.cleanup();
        return model;
    }

    private void runAdjust(Model model, Path dump, String logdir, List<Integer> reqIDs) throws IOException, IloException {
        int duration = (int)config.get("duration");
        prevDCs = null;
        System.out.println("\n *** Handle actual demand at interval " + t + " *** \n");
        lpWriter.println("\n *** Handle actual demand at interval " + t + " *** \n");

        List<Request> curRequests;
        curRequests = model.analyzeCurrentDemand(requests.get(t));
        List<Integer> curReqIDs = curRequests.stream().map(Request::getId).collect(Collectors.toList());
        List<Request> curRequestsArray = genWeightedRequests(curRequests, null,null,
                null, dcs,config, false, 0);
        if (curReqIDs.size() != curRequestsArray.size())
            throw new IllegalStateException("curReqIDs.size() != curRequestsArray.size()");
        if (t + 1 == duration) {
            List<Request> origRequests = new ArrayList<>(requests.get(t));
            printRunElements(dump, dcs, apps, functions, curRequestsArray,origRequests);
        }
        List<Request> origRequests;
        Model curDemandModel;
        if(logdir.contains("full") || logdir.contains("location_error")) {
            curDemandModel = new Model(model.getDcs(), apps, functions, model.getLinks(),
                    curRequestsArray, curRequests, curReqIDs, config, t, false, expType);
        }
        else{
            if (applyLowRevenueHeuristic) {
                Pair<List<Request>,Double> retPair = removeLowRevenueRequests(config, model.getDcs(), curRequestsArray,
                        curRequests, actCapRatio, actMaxLimit, reqTh,prevActReqs , "Actual");
                origRequests = retPair.getValue0();
                actCapRatio = retPair.getValue1();
                prevActReqs=curRequestsArray.size();
            }
            else
                origRequests = curRequests;
            model.cleanLargeSetsInModel();
            curDemandModel = new Model(model.getDcs(), apps, functions, model.getLinks(),
                    curRequestsArray, origRequests, curReqIDs, config, t, false, expType);
        }
        curDemandModel.solve(false);
        curDemandModel.getSolution();
        curDemandModel.analyzeUtilizedCapacities();
        curDemandModel.updateDCStateCosts();
        prevDCs = curDemandModel.getDcs();
        runtimeWriter.println(curDemandModel.getTime() + "," + curDemandModel.getType() +
                "," + (int) curDemandModel.getDetRunTime() + "," + curDemandModel.getCplexRunTime() +
                "," + actCapRatio + "," +
                curDemandModel.getOrigRequests().size());

        //Adjust capacity ratio by cplex solver gap from optimal
        if(!logdir.contains("full") && !logdir.contains("location_error")){
            System.out.println();
            double gap = curDemandModel.getGap();
            double[] pair = updateGap(gap,acceptableGap,actCapRatio, actMaxLimit, curDemandModel);
            actCapRatio = pair[0];
            actMaxLimit = pair[1];
        }
        curDemandModel.cleanup();
        reqIDs.clear();
    }

    private HashMap<Integer, DC> cloneDCs(HashMap<Integer, DC> prevDCs){
        HashMap<Integer, DC> dcsCP = new HashMap<Integer, DC>();
        for(Map.Entry<Integer, DC> kv : dcs.entrySet()){
            Integer d = kv.getKey();
            DC v = kv.getValue();
            DC dc1 = new DC(v.getId(),v.getX(),v.getY(),v.getCapacity(),v.getProcPower(),v.getStateCost(),
                    v.getOriginalCapacity(),v.getNeigbors());
            dcsCP.put(dc1.getId(),dc1);
            //get previous placements
            if (prevDCs != null) {
                dcsCP.get(d).setExistingState(prevDCs.get(d).getExistingState());
                dcsCP.get(d).setStateCost(prevDCs.get(d).getStateCost());
            }
        }
        return dcsCP;
    }

    private HashMap<Pair<Integer, Integer>, Link> cloneLinks(){
        HashMap<Pair<Integer, Integer>, Link> linksCP = new HashMap<Pair<Integer, Integer>, Link>();
        for(Map.Entry<Pair<Integer,Integer>, Link> kv : links.entrySet()) {
            Pair<Integer, Integer> k = kv.getKey();
            Link v = (Link) kv.getValue();
            Link link1 = new Link(v.getNode1(),v.getNode2(),v.getDelay(),v.getBw());
            linksCP.put(k,link1);
        }
        return linksCP;
    }

    public static double[] updateGap(double gap, double acceptableGap, double capRatio, double maxLimit, Model model){
        double[] result = new double[2];
        if (gap < acceptableGap) {
            if(capRatio<=1.5)
                capRatio += 0.25;

        }
        else if(gap > 1 && capRatio>=2)
            capRatio *= 0.75;
        else if(gap > 10*acceptableGap) {
            double reduction = 1;
            if (capRatio>reduction)
                capRatio -= reduction/2;
            else if (capRatio>reduction/2)
                capRatio -= reduction/4;
            else if (capRatio>reduction/4)
                capRatio -= reduction/8;
            else if (capRatio>reduction/16)
                capRatio -= reduction/16;
        }

        if (model.getNumOfRequests() > 100 && model.getNumOfAllocated() < 10 &&
                model.getTime() >= 3 && maxLimit > model.getNumOfRequests()) {
            maxLimit = model.getNumOfRequests()-20;
            if(maxLimit<0)
                throw new IllegalArgumentException("maxLimit < 0");
            System.out.println("Gap updated to " + maxLimit);
        }
        result[0] = capRatio;
        result[1] = maxLimit;
        return result;



    }

    private static List<Request> genWeightedRequests(List<Request> requests, List<Request> requestsActual,
                                                     double[][] P,
                                                     HashMap<Integer, List<Double>> PLW, HashMap<Integer, DC> dcs,
                                                     HashMap<String, Object> config, boolean estimated, double pThreshold){
        double locationErrorRate=0;
        HashMap<Integer,ArrayList<Request>> requestHashMap = new HashMap<>();
        List<Double> probsLW = new ArrayList<>();
        List<Double> newProbsLW = new ArrayList<>();
        boolean locationErrorModel = (Boolean)config.get("locationErrorModel");
        boolean limitUserReqs = (Boolean)config.get("limitUserReqs");
        int initialRealisticCapacity = (int)config.get("initialRealisticCapacity");
        double levyWalkExitRate = (double)config.get("levyWalkExitRate");
        boolean levyWalk = (Boolean)config.get("levyWalk");
        if (locationErrorModel)
            locationErrorRate = (Double) config.get("locationErrorRate");
        boolean[][] dcsInRange = null;
        if (levyWalk)
            dcsInRange = Link.collectDcsWithinRange(dcs, (int) config.get("allowedRange"));

        RandomGenerator rand = new Well19937c(((int)config.get("seed")));
        int numOfDCs = dcs.size();
        List<Request> newRequests = new ArrayList<>();
        HashMap<Integer,Integer> actualLocation = null; //<r,d>

        if (requestsActual!=null){
            actualLocation = new HashMap<>();
            for (Request request : requestsActual){
                actualLocation.put(request.getId(),request.getLocation());
            }
        }
        //Divide requests by locations
        for (Request request : requests){
            ArrayList<Request> requestList = new ArrayList<>();
            int rID = request.getId();
            if(request.getLocation()>numOfDCs)
                throw new IllegalArgumentException("DC doesn't exist: " + request.getLocation());

            float f = rand.nextFloat();
            boolean locationError = false;
            if (f<locationErrorRate)
                locationError = true;
            if(P == null && PLW==null){ //handling real demand
                int d = request.getLocation();
                int revenue = request.getRevenue();
                if (requestsActual!=null) { //if has actual locations, use it
                    if (actualLocation.containsKey(rID)) //if key not contained, request no longer exists
                        d = actualLocation.get(rID);
                    else
                        revenue=0; //if no longer exists -> revenue=0
                }
                //Set false location
                if (locationError && estimated){
                    d = (d+1) % dcs.size();
                }
                Request newReq = new Request(request.getTime(),rID,d, request.getAppS(), revenue);
                newReq.setWeight(1);
                newReq.setApp(request.getApp());
                newReq.setTotalWeight(1);

                newRequests.add(newReq);
                request.setTotalWeight(1);
                continue;
            }

            int srcNode = request.getLocation();
            double totalWeight=0;
            //Sum all probabilities from src
            if(!levyWalk) {
                for (int dstNode = 0; dstNode < dcs.size(); dstNode++) {
                    if (P[srcNode][dstNode] > pThreshold) {
                        totalWeight += P[srcNode][dstNode];
                    }
                }
            }
            else {
                newProbsLW = getLegalNormalizedLwProbs(PLW, request, dcsInRange, levyWalkExitRate);
                totalWeight = newProbsLW.stream().mapToDouble(a -> a.doubleValue()).sum();
            }
            double minP=1;
            double maxP=0;
            int pCount=0;
            //get min and max probabilities from srcNode
            if (locationError){
                for(int dstNode=0; dstNode<dcs.size();dstNode++)
                    if(P[srcNode][dstNode] >0){
                        pCount++;
                        if (P[srcNode][dstNode]<minP)
                            minP = P[srcNode][dstNode];
                        if (P[srcNode][dstNode] > maxP) {
                            maxP = P[srcNode][dstNode];
                        }
                    }
            }
            for(int dstNode=0; dstNode<dcs.size();dstNode++){
                //for each dstNode take P to reach it from srcNode
                if((!levyWalk && P[srcNode][dstNode] >pThreshold) || (levyWalk && newProbsLW.get(dstNode)>pThreshold)){
                    Request newReq = new Request(request.getTime(),request.getId(),dstNode, request.getAppS(),request.getRevenue());

                    //increased max
                    if(locationError){
                        if (P[srcNode][dstNode]==maxP) {
                            double newP = P[srcNode][dstNode]  + 0.05 * (pCount - 1);
                            BigDecimal newPBig = new BigDecimal(newP);
                            newPBig = newPBig.round(new MathContext(2, RoundingMode.HALF_UP));
                            newReq.setWeight(newPBig.doubleValue());
                        }
                        else {
                            if (P[srcNode][dstNode] <= 0.05)
                                continue;
                            double newP = P[srcNode][dstNode] - 0.05;
                            BigDecimal newPBig = new BigDecimal(newP);
                            newPBig = newPBig.round(new MathContext(2, RoundingMode.HALF_UP));
                            newReq.setWeight(newPBig.doubleValue()); //P[srcNode][dstNode] - 0.05
                        }
                    }
                    else if(!levyWalk)
                        newReq.setWeight(P[srcNode][dstNode]);
                    else if(levyWalk)
                        newReq.setWeight(newProbsLW.get(dstNode));
                    newReq.setApp(request.getApp());
                    BigDecimal totalWeightBig = new BigDecimal(totalWeight);
                    totalWeightBig = totalWeightBig.round(new MathContext(2, RoundingMode.HALF_UP));
                    if(totalWeightBig.doubleValue()==0)
                        throw new IllegalArgumentException("Total weight of request is 0");
                    newReq.setTotalWeight(totalWeightBig.doubleValue());
                    request.setTotalWeight(totalWeightBig.doubleValue());
                    request.setWeight(totalWeightBig.doubleValue());
                    newRequests.add(newReq);
                }
            }
            requestHashMap.put(request.getId(),requestList);
        }
        if(estimated && limitUserReqs) {
            newRequests.sort(Comparator.comparingDouble(Request::getWeight));
            Collections.reverse(newRequests);
            newRequests = newRequests.stream().limit(initialRealisticCapacity).collect(Collectors.toList());
        }
        return newRequests;

    }

    private static List<Double> getLegalNormalizedLwProbs(HashMap<Integer, List<Double>> PLW, Request request,
                                                          boolean[][] dcsInRange, double levyWalkExitRate){
        double totalLwProb = 1;
        if (levyWalkExitRate!=0)
            totalLwProb -= levyWalkExitRate;
        boolean changedVector = false;
        int userLoc = request.getLocation();
        List<Double> probsLW = new ArrayList<>(PLW.get(request.getId()));
        for (int d=0; d< probsLW.size(); d++){
            if (probsLW.get(d) > 0 && !dcsInRange[userLoc][d]) {
                probsLW.set(d, 0.0);
                changedVector = true;
            }
        }

        double sum = probsLW.stream().mapToDouble(Double::doubleValue).sum() / totalLwProb;
        return probsLW.stream()
                .map(d -> d / sum)
                .collect(Collectors.toList());
    }

    /**
     * Receives expected users locations and normalizes to current locations
     * @param neighbors
     * @param probsLW
     * @return
     */
    private static List<Double> getLegalProbsLW(List<Integer> neighbors, List<Double> probsLW ){
        Double[] data = new Double[probsLW.size()];
        Arrays.fill(data, (double) 0);
        List<Double> newProbsLW = Arrays.asList(data);
        double totalWeight = probsLW.stream().mapToDouble(f -> f.doubleValue()).sum();
        for (int d=0;d<probsLW.size();d++){
            if(neighbors.contains(d))
                newProbsLW.set(d,probsLW.get(d));
        }
        double newTotalWeight = newProbsLW.stream().mapToDouble(f -> f.doubleValue()).sum();
        if(totalWeight!=newTotalWeight) {
            double adjustedRatio = totalWeight / newTotalWeight;
            for (int d = 0; d < newProbsLW.size(); d++)
                newProbsLW.set(d, adjustedRatio * newProbsLW.get(d));
        }
        return newProbsLW;
    }



    private void writeLpSummary(String run) throws IOException {
//        String logdir = fileHandler.getLogdir(run);
//        Path p = Paths.get(logdir);
//        Path dump = fileHandler.getDumpdir(run);
//        Path reqPath = Paths.get(dump +"/requests");
//        if(Files.exists(p))
//            Files.walk(p)
//                    .sorted(Comparator.reverseOrder())
//                    .map(Path::toFile)
//                    .forEach(File::delete);
//        Files.createDirectories(p);
//        Files.createDirectories(dump);
//        Files.createDirectories(reqPath);
        Path dump = fileHandler.getDumpdir();
        Path logdir = fileHandler.getLogdir();
        runtimeWriter = new PrintWriter(new FileOutputStream(
                new File(dump+"/runtime.txt"),true),true);
        runtimeWriter.println("time,type,runtime,runtime2,capacityRatio,requests");
        lpWriter = new PrintWriter(new FileOutputStream(
                new File(logdir+"/lp_summary_t1_Prealloc.txt"),
                true /* append = true */),true);
        lpWriter.println("d(r,d,f)");
        lpWriter.println("dd(r,src d,placed d,f)");
        lpWriter.println("y(r,p,l)");
        lpWriter.println("yy(r,src d,p,l)");
        lpWriter.println("s(r,d,p)");
        lpWriter.println();
        lpWriter.println("Functions:");
        for(int f=1;f< functions.size();f++){
            lpWriter.println("ID: " + functions.get(f).getId() + ", size: " + functions.get(f).getProcSize());
        }
        lpWriter.println();

    }

    private void mamiRun(){
        int duration = (int)config.get("duration");
        probMapMAMI = Probabilities.readLocationProbsCSV(mamiThreshold);
//        String logdir = fileHandler.getLogdir();
//        Path p = Paths.get(logdir);
//        Path dump = fileHandler.getDumpdir();
//        Path reqPath = Paths.get(dump +"/requests");
//        try {
//            if (Files.exists(p))
//                Files.walk(p)
//                        .sorted(Comparator.reverseOrder())
//                        .map(Path::toFile)
//                        .forEach(File::delete);
//            Files.createDirectories(p);
//            Files.createDirectories(dump);
//            Files.createDirectories(reqPath);
//        }
//        catch (IOException e){
//            e.printStackTrace();
//        }
        Path dump = fileHandler.getDumpdir();
        HashMap<Integer, HashMap<Integer, HashMap<Integer, vnfPlace>>> mamiPlacement = null;
        HashMap<Integer, DC> itDCs = null;
        HashMap<Integer, DC> physicalDCs = null;
        for (int t = 0; t < duration; t++) {
            //Sort by profit
            List<Request> curRequests = new ArrayList<>(requests.get(t));
            curRequests.sort(Comparator.comparingInt(Request::getRevenue));
            Collections.reverse(curRequests);
            HashMap<Pair<Integer, Integer>, Link> linksCP = cloneLinks();

            if(itDCs==null)
                itDCs=dcs; //first iteration
            else
                itDCs=physicalDCs; //ignore all dynamic placements

            try {
                printRunElements(dump, dcs, apps, functions, curRequests, curRequests);
            }
            catch (IOException e){
                e.printStackTrace();
            }
            MamiModel model = new MamiModel(curRequests,functions,apps,itDCs, linksCP, probMapMAMI, probsLW,
                    mamiPlacement, config, t);

            //clone DCs - Keep physical placement
            physicalDCs = new HashMap<>();
            for(Map.Entry<Integer, DC> kv : itDCs.entrySet()) {
                DC v = kv.getValue();
                DC dc1 = new DC(v.getId(),v.getX(),v.getY(),v.getCapacity(),v.getProcPower(),v.getStateCost(),
                        v.getOriginalCapacity(),v.getNeigbors());
                physicalDCs.put(dc1.getId(),dc1);
            }
            model.processRequests();
            try {
                model.getSolution();
            }
            catch (FileNotFoundException e){
                e.printStackTrace();
            }
            mamiPlacement = model.getPlacement();
        }
        System.gc();
        System.out.println("Run completed");
    }

    public Pair<List<Request>,Double> removeLowRevenueRequests(
            HashMap<String, Object> config, HashMap<Integer, DC> dcs, List<Request> requests, List<Request> originRequests,
            double capacityRatioThreshold, double reqMaxLimit, double reqTh, int prevNumOfReqs, String type) throws FileNotFoundException {
        HashMap<Integer,List<Request>> nodeDemand = new HashMap<>();
        HashMap<Pair<Integer,Integer>,Request> reqMap = new HashMap<>(); //<(r,d),Request> (estimated requests)
        HashMap<Integer,Request> origReqMap = new HashMap<>(); //<reqID, Request>
        int[] demandSize = new int[dcs.size()];
        Arrays.fill(demandSize,0);
        for(int d=0;d<dcs.size();d++){
            List<Request> reqList = new ArrayList<>();
            nodeDemand.put(d,reqList);
        }
        //map of original requests
        for(Request req: originRequests)
            origReqMap.put(req.getId(),req);
        for(Request req: requests){
//        for(int r=0; r<requests.size(); r++){
//            Request req = requests.get(r);
            int d = req.getLocation();
            if(reqMap.get(Pair.with(req.getId(),d)) != null)
                throw new IllegalStateException("Non empty map for (r,d) = " + req.getId() + "," + d);
            reqMap.put(Pair.with(req.getId(),d),req);
            List<Request> reqSet = nodeDemand.get(d);
            reqSet.add(req);
//            nodeDemand.put(d,reqSet);
            demandSize[d] += req.getApp().getTotalSize();
        }

        //sort by revenue
        for(int d=0; d< dcs.size();d++){
            if (nodeDemand.get(d).size()==0)
                continue;
            List<Request> reqList = nodeDemand.get(d);
            reqList.sort(Comparator.comparingInt(Request::getRevenue));
            //remove requests with the lowest revenue until capacity threshold reached
            while(demandSize[d] > dcs.get(d).getCapacity() * capacityRatioThreshold){
                demandSize[d] -= reqList.get(0).getApp().getTotalSize();
                reqMap.get(Pair.with(reqList.get(0).getId(),d)).setUnused();
                reqMap.remove(Pair.with(reqList.get(0).getId(),d));
                reqList.remove(0);
            }
        }
        int allowedReqGap=100;
        int actualReqGap = reqMap.size() - prevNumOfReqs;
        int requestsToDrop = Math.max(actualReqGap-allowedReqGap, reqMap.size() - (int)reqMaxLimit);
        if(prevNumOfReqs>0 && (requestsToDrop > 0)){
            ArrayList<Request> tempReqs = new ArrayList<>(reqMap.values());
            tempReqs.sort(Comparator.comparingDouble(Request::getWeightedRevenue));
            for(int i=0;i<requestsToDrop;i++){
                Request r = tempReqs.get(i);
                reqMap.get(Pair.with(r.getId(),r.getLocation())).setUnused();
                reqMap.remove(Pair.with(r.getId(),r.getLocation()));
            }
        }


        //check if portions of request remained
        Iterator it = origReqMap.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry kv = (Map.Entry) it.next();
            int id = (int)kv.getKey();
            boolean exists=false;
            float totalWeight=0;
            for(int d=0; d< dcs.size();d++){
                if(reqMap.containsKey(Pair.with(id,d))) {
                    exists = true;
                    totalWeight += reqMap.get(Pair.with(id,d)).getWeight();
                    if (totalWeight>=reqTh)
                        break;
                }
            }
            if(!exists) //if request fully removed or below th, remove it from original requests
                it.remove();
            else if (totalWeight<reqTh){
                it.remove();
                for(int d=0; d< dcs.size();d++) {
                    reqMap.remove(Pair.with(id, d));
//                    }
                }
            }
        }
        Path dumpPath = fileHandler.getDumpdir();
        printEstimatedHeuristicRequests(dumpPath, requests, type);
        //copy map back to list
        requests.clear();
        requests.addAll(reqMap.values());
        List<Request> origSet = new ArrayList<>(origReqMap.values());
        return Pair.with(origSet,capacityRatioThreshold);
    }

    public static void printEstimatedHeuristicRequests(Path dumpPath, List<Request> estRequests, String type) throws FileNotFoundException {
        File estReqFile = new File(dumpPath.toString()+"/preallocHeuristicRequests.csv");
        PrintWriter writer = new PrintWriter(new FileOutputStream(estReqFile,true));
        if(estReqFile.length()==0)
            writer.println("time,type,reqID,dc,appID,weight,totalWeight,size,revenue,used");
        estRequests.sort(Comparator.comparingInt(Request::getId));
        for(Request request:estRequests){
            writer.println(request.getTime() + "," +type + "," + request.getId() +
                    "," + request.getLocation() +"," +request.getAppS()+ "," + request.getWeight() + "," +
                    request.getTotalWeight() + "," + request.getApp().getTotalSize() +
                    "," + request.getRevenue() +"," + request.getUsed());
        }
        writer.close();
    }


    public void printRunElements(Path dumpPath, HashMap<Integer, DC> dcs, HashMap<Integer,App> apps,
                                        HashMap<Integer, Function> functions, List<Request> estRequests,
                                        List<Request> origRequests) throws IOException {
        File dcFile = new File(dumpPath.toString()+"/datacenters.csv");
        if(dcFile.length()==0) {
            PrintWriter writer = new PrintWriter(new FileOutputStream(dcFile,
                    true /* append = true */));
            writer.println("dc,x,y,capacity,procPower");
            for (int d = 0; d < dcs.size(); d++) {
                DC dc = dcs.get(d);
                writer.println(dc.getId() + "," + dc.getX() + "," + dc.getY() +
                        "," + dc.getCapacity() + "," + dc.getProcPower());
            }
            writer.close();
        }

        File appFile = new File(dumpPath +"/applications.csv");
        if(appFile.length()==0) {
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    appFile, false));
            writer.println("appID,functions,constraints,size");
            for (Map.Entry<Integer, App> kv : apps.entrySet()) {
                App app = kv.getValue();
                String funcList = "";
                String ctList = "";
                for (int p = 0; p < app.getPairs().size() - 1; p++) {
                    Pair<Integer, Integer> pair = app.getPairs().get(p);
                    int pairCT = app.getPairCTs().get(p);
                    funcList = funcList.concat(String.valueOf(pair.getValue1()));
                    funcList = funcList.concat("-");

                    if (p != 0) {
                        ctList = ctList.concat(String.valueOf(pairCT));
                        ctList = ctList.concat("-");
                    }

                }
                funcList = funcList.substring(0, funcList.length() - 1); //remove last "-"
                ctList = ctList.substring(0, ctList.length() - 1); //remove last "-"
                writer.println(app.getId() + "," + funcList + "," + ctList + "," + app.getTotalSize());
            }
            writer.close();
        }

        File funcFile = new File(dumpPath +"/functions.csv");
        if(funcFile.length()==0) {
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    funcFile, false));
            writer.println("id,size,launchTime");
//            Iterator it = functions.entrySet().iterator();
//            while (it.hasNext()) {
            for(Map.Entry<Integer, Function> kv : functions.entrySet()){
//                Map.Entry kv = (Map.Entry) it.next();
                Function function = kv.getValue();
                writer.println(function.getId() + "," + function.getProcSize());
            }
            writer.close();
        }

        File origReqFile = new File(dumpPath +"/originalRequests.csv");
        PrintWriter writer = new PrintWriter(new FileOutputStream(origReqFile,true ));
        if(origReqFile.length()==0)
            writer.println("time,reqID,dc,appID,size,revenue");
        List<Request> sortedList = new ArrayList<>(origRequests);
        sortedList.sort(Comparator.comparingInt(Request::getId));
        for(Request request:origRequests){
            writer.println(request.getTime() + "," + request.getId() + "," + request.getLocation() +
                    "," +request.getAppS()+ "," + request.getApp().getTotalSize() + "," + request.getRevenue());
        }
        writer.close();

        File estReqFile = new File(dumpPath +"/preallocRequests.csv");
        writer = new PrintWriter(new FileOutputStream(estReqFile,true));
        if(estReqFile.length()==0)
            writer.println("time,reqID,dc,appID,weight,totalWeight,size,revenue");
        sortedList = new ArrayList<>(estRequests);
        sortedList.sort(Comparator.comparingInt(Request::getId));
        for(Request request:sortedList){
            writer.println(request.getTime() + "," + request.getId() + "," + request.getLocation()
                    +"," +request.getAppS()+ "," + request.getWeight() + "," +
                    request.getTotalWeight() + "," + request.getApp().getTotalSize() + "," + request.getRevenue());
        }
        writer.close();
        if(levyWalk){
            Files.copy(Paths.get("include/userProbsLW.csv"), Paths.get(dumpPath + "/userProbsLW.csv"),
                    StandardCopyOption.REPLACE_EXISTING);
            Files.copy(Paths.get("include/requestsLW.xlsx"), Paths.get(dumpPath + "/requests/requestsLW.xlsx"),
                    StandardCopyOption.REPLACE_EXISTING);
        }
        else {
            Files.copy(Paths.get("include/probabilities.xlsx"), Paths.get(dumpPath + "/probabilities.xlsx"),
                    StandardCopyOption.REPLACE_EXISTING);
            Files.copy(Paths.get("include/requests.xlsx"), Paths.get(dumpPath + "/requests/requests.xlsx"),
                    StandardCopyOption.REPLACE_EXISTING);
        }

        Files.copy(Paths.get("include/Req2GenericReqIdMap.txt"), Paths.get(dumpPath +"/requests/Req2GenericReqIdMap.txt"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get("include/config.xml"), Paths.get(dumpPath +"/config.xml"), StandardCopyOption.
                REPLACE_EXISTING);
        Files.copy(Paths.get("include/cplex_config.prm"), Paths.get(dumpPath +"/cplex_config.prm"),
                StandardCopyOption.REPLACE_EXISTING);
    }

    public static double round1(double value, int scale) {
        return Math.round(value * Math.pow(10, scale)) / Math.pow(10, scale);
    }

    private void createRundir(){
        try {
            Path p = getPathToRunlogs();
            if (Files.exists(p))
                Files.walk(p)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            Files.createDirectories(p);
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private Path getPathToRunlogs(){
        return Paths.get("runlogs");
    }
}