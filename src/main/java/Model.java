import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.util.*;

import Inputs.*;
import ilog.concert.*;
import ilog.cplex.*;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Triplet;

import java.util.stream.Collectors;

public class Model {
    private String type;
    private int time;
    private Double reqTh;
    private HashMap<Integer, Function> functions;
    private HashMap<Integer, App> apps;
    private HashMap<Integer, DC> dcs;
    private final HashMap<Pair<Integer,Integer>, Link> links;
    private final HashMap<Pair<Integer,Integer>, IloNumExpr> sdMap;
    private HashMap<Pair<Integer,Integer>, List<Request>> reservedApps; //k:<d,app>
    FileHandler fileHandler;

    private int numOfAllocated = 0;
    private final HashMap<Integer, IloNumExpr> sdReqMap; //<r,d>
    private List<Request> requests;
    private List<Request> origRequests;
    private HashMap<Integer,Boolean> reqIDs; //k:<id>, v:reserved
    private HashMap<Integer,Request> reqMap; //r, Request
    private HashMap<Pair<Integer,Integer>,Boolean> placed_; //k:<id,d>, v:reserved
    private ArrayList<IloNumVar> ytrafficArray;
    private ArrayList<IloConstraint> constraints;
    HashMap<String,Object> config;
    int[] initialCapacities;
    int allowedRange;
    IloNumExpr totalStateCost;
    private boolean debugMode;
    private boolean placeAtUserLoc;
    private boolean nonSplitMode;
    private boolean checkBrokenChains;
    //temporary store (linked list to maintain HashMap determinism)
    LinkedHashMap<IloNumVar,IloConstraint> ddMap;

    private boolean gotSolution;

    boolean[][] dcsInRange = null;


    //constants
    private HashMap<Pair<Integer,Integer>,Integer> procDelayConst; // <d,f>
    //Variables
    private HashMap<Triplet<Integer,Integer,Integer>,IloNumVar> deltaPlaced; //<r,d,f>
    private HashMap<Triplet<Integer,Integer,Integer>,Pair<IloNumVar,Integer>> deltardPlaced; //<(r,d'),d,f>, 1 in value marks valid
    private HashMap<Triplet<Integer,Integer,Pair>,IloNumExpr> ytrafficPlaced; //<r,p,l>
    private HashMap<Triplet<Integer,Integer,Pair>,IloNumExpr> ytrafficdPlaced; //<r,d',p,l>
    //r,d' to r'
    private HashMap<Pair<Integer,Integer>,Integer> rd2r_;
    private HashMap<Integer,Pair<Integer,Integer>> r_2rd;
    private String logdir;
    private IloCplex cplex;

    public Model(HashMap<Integer, DC> dcs, HashMap<Integer, App> apps, HashMap<Integer, Function> functions,
                 HashMap<Pair<Integer,Integer>, Link> links, List<Request> requests, List<Request> originRequests,
                 List<Integer> reqIDs, HashMap<String,Object> config, int time, boolean estimated, String expType) throws IloException, FileNotFoundException {
        this.cplex = new IloCplex();
        this.functions = functions;
        this.apps = apps;
        this.dcs = dcs;
        this.links = links;
        this.requests = requests;
        this.config = config;
        this.reqTh = (Double) this.config.get("threshold");
        this.origRequests = originRequests;
        this.deltaPlaced = new HashMap<>();
        this.deltardPlaced = new HashMap<>();
        this.ytrafficPlaced = new HashMap<>();
        this.ytrafficdPlaced = new HashMap<>();
        this.procDelayConst = new HashMap<>();
        this.sdMap = new HashMap<>();
        this.reservedApps = new HashMap<>();
        this.constraints = new ArrayList<>();
        this.ytrafficArray = new ArrayList<>();
        this.sdReqMap = new HashMap<>();
        this.rd2r_ = new HashMap<>();
        this.r_2rd = new HashMap<>();
        this.reqIDs = new HashMap<>();
        this.reqMap = new HashMap<>();
        this.fileHandler = new FileHandler(config, expType);
        this.allowedRange = (int) config.get("allowedRange");
        this.debugMode = (boolean) config.get("debugMode");
        this.placeAtUserLoc = (Boolean) this.config.get("placeAtUserLoc");
        this.nonSplitMode = (Boolean) this.config.get("nonSplitMode");
        this.checkBrokenChains = (Boolean)config.get("checkBrokenChains");
        this.ddMap = new LinkedHashMap<>();
        this.gotSolution = false;
//        this.expType = expType;
        this.logdir = String.valueOf(fileHandler.getLogdir());

        for(int req: reqIDs)
            this.reqIDs.put(req,false);

        initialCapacities = new int[dcs.size()];
        for(int d=0;d< dcs.size();d++)
            initialCapacities[d] = dcs.get(d).getCapacity();

        type = estimated ? "Preallocate" : "Adjust";
        this.time = time;
        File attemptedFile = new File(this.logdir+"/dump/attemptedRequests.csv");
        PrintWriter AttemptedWriter = new PrintWriter(new FileOutputStream(attemptedFile, true),true);
        if(attemptedFile.length()==0)
            AttemptedWriter.println("time,type,reqID");
        for(Request request:originRequests)
            AttemptedWriter.println(time +","+type +","+ request.getId());

        // Initialize the model
        createrd2r();
        addProcDelayConst();
        addDeltaPlacedVariables(originRequests);
        addyTrafficVariables(originRequests);
        addConstraints(originRequests);
        addObjective(originRequests);

        if (debugMode)
            cplex.exportModel(this.logdir+"/model_" + "t" + this.time + "_" + type + ".lp");

        // Optionally: suppress the output of CPLEX
        cplex.setOut(new PrintStream(this.logdir+"/run_" + "t" + this.time + "_" + type + ".log"));

    }


    public void cleanLargeSetsInModel(){
        ytrafficdPlaced.clear();
        ytrafficPlaced.clear();
        ytrafficArray.clear();
        ytrafficdPlaced.clear();
        deltaPlaced.clear();
        deltardPlaced.clear();
        cleanup();
    }
    static public void measureDuration(long startTime, String measuredName){
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        double elapsedTimeInSecond = (double) duration / 1_000_000_000;
        System.out.println(measuredName + ": " + elapsedTimeInSecond);
    }

    private void createrd2r(){
        for(int r=0;r< requests.size();r++){
            int rid = requests.get(r).getId();
            int d = requests.get(r).getLocation();
            rd2r_.put(Pair.with(rid,d),r);
            r_2rd.put(r, Pair.with(rid,d));
        }
    }

    /**
     * Creates boolean variable to mark placement of functions in dcs
     * @throws IloException if something wrong with Cplex
     */
    private void addDeltaPlacedVariables(List<Request> originRequests) throws IloException
    {
        for(Request req: originRequests){
            int r = req.getId();
            reqMap.put(r, req);
            for (int d=0; d< dcs.size(); d++){
                for (int f=1; f<= functions.size(); f++){
                    String str = "d("+ r +","+ d +","+ f +")";
                    if(!req.getApp().getAppFunctions().contains(f))
                        continue;
                    if(!dcs.get(d).containsExistingState(r,f)) //add new state in d
                        dcs.get(d).addExistingState(r,f);
                    IloNumVar var = cplex.boolVar(str);
                    deltaPlaced.put(Triplet.with(r,d,f), var);
                    for (int dd=0; dd< dcs.size(); dd++){
                        if(!rd2r_.containsKey(Pair.with(r,dd)))
                            continue;
                        String str2 = "dd("+ r +","+ dd +","+ d +","+ f +")";
                        IloNumVar vardd = cplex.boolVar(str2);
                        int r_ = rd2r_.get(Pair.with(r,dd));
                        //avoid placing unused functions
                        IloConstraint c = null;
                        //if not within range
                        if (!DC.areDCsInRange(dcs.get(d),dcs.get(dd),allowedRange))
                            deltardPlaced.put(Triplet.with(r_,d,f),Pair.with(vardd,0)); //invalidate this delta
                        else {
                            c = cplex.ge(var, vardd, str2 + "_delta_ge");
                            deltardPlaced.put(Triplet.with(r_,d,f),Pair.with(vardd,1));
                        }
                        ddMap.put(vardd,c); //added after cleanup
                    }
                }
            }
        }
    }


    /**
     * Creates boolean variable to mark traffic between placed functions
     * @throws IloException if something wrong with Cplex
     */
    private void addyTrafficVariables(List<Request> originRequests) throws IloException
    {
        dcsInRange = Link.collectDcsWithinRange(dcs, allowedRange);
        //Place all in same dc
//        for (int r=0; r< requests.size(); r++) {
        for(Request req : requests){
            //add ytraffic only once for each req
            int rid = req.getId();
            int d = req.getLocation();
            int r_ = rd2r_.get(Pair.with(rid,d));
            HashMap<Integer,Pair<Integer,Integer>> requestAppPairs = req.getApp().getPairs();
            for (int p=0; p< requestAppPairs.size(); p++){ //iterate over function pairs
                Pair<Integer, Integer> funcPair = requestAppPairs.get(p);
                int f = funcPair.getValue0();
                int g = funcPair.getValue1();
                //for each link
                for (Map.Entry<Pair<Integer, Integer>, Link> kv : links.entrySet()) {
                    Pair<Integer, Integer> link = kv.getKey();
                    boolean inRange = dcsInRange[link.getValue0()][link.getValue1()];
                    Triplet<Integer, Integer, Integer> del1 = Triplet.with(r_, link.getValue0(), f);
                    Triplet<Integer, Integer, Integer> del2 = Triplet.with(r_, link.getValue1(), g);
                    Pair<IloNumVar,Integer> del1Pair = deltardPlaced.get(del1);
                    Pair<IloNumVar,Integer> del2Pair = deltardPlaced.get(del2);

                    String str = "yy(" + rid + "," + d + "," + p + ",{" + link.getValue0() +
                            "," + link.getValue1() + "})";
                    if ((kv.getValue()).getBw() == 0 || placeAtUserLoc) {
                        ytrafficPlaced.put(Triplet.with(rid, p, link), cplex.constant(0));
                        ytrafficdPlaced.put(Triplet.with(r_, p, link), cplex.constant(0));
                        //when link is illegal, cannot have function placed on the link's other end
                        if (f == 0) {
                            if (link.getValue0() == d) {
                                ddMap.remove(del2Pair.getValue0());
                                IloNumVar v = del2Pair.getValue0();
                                deltardPlaced.put(del2, Pair.with(v, 0)); //invalidate this delta
                            }
                            continue;
                        } else if (g == 0) {
                            if (link.getValue1() == d) {
                                ddMap.remove(del1Pair.getValue0());
                                IloNumVar v = del1Pair.getValue0();
                                deltardPlaced.put(del1, Pair.with(v, 0)); //invalidate this delta
                            }
                            continue;
                        } else {
                            if (del1Pair.getValue1() == 1 && del2Pair.getValue1() == 1)
                                cplex.addGe(cplex.constant(1), cplex.sum(del1Pair.getValue0(), del2Pair.getValue0()), str + "_(3)"); //1 >= delta1+ delta2
                            continue;
                        }
                    }
                    //y variable generation
                    String stry = "y(" + rid + "," + p + ",{" + link.getValue0() +
                            "," + link.getValue1() + "})";
                    if (!ytrafficPlaced.containsKey(Triplet.with(rid, p, link))) {
                        IloNumVar vy = cplex.boolVar(stry);
                        ytrafficPlaced.put(Triplet.with(rid, p, link), vy);
                        ytrafficArray.add(vy);
                    }
                    IloNumExpr vary = ytrafficPlaced.get(Triplet.with(rid, p, link));

                    //yy generation
                    if (!ytrafficdPlaced.containsKey(Triplet.with(r_, p, link))) {
                        IloNumVar v = cplex.boolVar(str);
                        ytrafficdPlaced.put(Triplet.with(r_, p, link), v);
                        if (!inRange)
                            cplex.addEq(v, 0, str + "_yy_not_in_range");
                    }
                    IloNumExpr varyy = ytrafficdPlaced.get(Triplet.with(r_, p, link));

                    //for first pair add traffic from user to first functions
                    if (f == 0) {
                        //Only for links from user location
                        if (link.getValue0() == d) {
                            if (del2Pair.getValue1() == 1) {
                                if (inRange)
                                    cplex.addLe(varyy, del2Pair.getValue0(), str + "_(2)");   //y<=delta
                                cplex.addEq(vary, varyy, str + "_eq_first"); //y=yy only one yy
                                cplex.addGe(varyy, del2Pair.getValue0(), str + "_(3)"); //y+1 >= 1+ delta
                            }
                        } else
                            ytrafficdPlaced.put(Triplet.with(r_, p, link), cplex.constant(0));
                        continue;
                    }

                    //Traffic back to user
                    if (g == 0) {
                        //Only for links from user location
                        if (link.getValue1() == d) {
                            if (del1Pair.getValue1() == 1) {
                                if (inRange)
                                    cplex.addLe(varyy, del1Pair.getValue0(), str + "_(1)");   //y<=delta
                                cplex.addEq(vary, varyy, str + "_eq_last"); //y=yy only one yy
                                cplex.addGe(varyy, del1Pair.getValue0(), str + "_(3)"); //y+1 >= 1+ delta
                            }
                        } else
                            ytrafficdPlaced.put(Triplet.with(r_, p, link), cplex.constant(0));
                        continue;
                    }

                    /* y<= del1*h (1)
                     *  y<= del2*h (2)
                     *  y +1 >= del1 + del2 (3) */
                    int pairExists = req.getApp().checkPair(f, g);

                    if (del1Pair.getValue1() == 0) { //del1==0
                        ytrafficdPlaced.put(Triplet.with(r_, p, link), cplex.constant(0));
                        continue;
                    } else if (inRange)
                        cplex.addLe(varyy, cplex.prod(del1Pair.getValue0(), pairExists), str + "_(1)");

                    if (del2Pair.getValue1() == 0) {//del2==0
                        ytrafficdPlaced.put(Triplet.with(r_, p, link), cplex.constant(0));
                        continue;
                    } else if (inRange)
                        cplex.addLe(varyy, cplex.prod(del2Pair.getValue0(), pairExists), str + "_(2)");

                    if (del1Pair.getValue1() == 1 && del2Pair.getValue1() == 1) {
                        cplex.addGe(cplex.sum(varyy, 1), cplex.sum(del1Pair.getValue0()
                                , del2Pair.getValue0()), str + "_(3)");
                        cplex.addGe(vary, varyy, str + "_le"); //y>=yy
                    }
                }
            }
        }

        //add delta sum post cleanup
        for(Request req: originRequests) {
            int r = req.getId();
            for (int f = 1; f <= functions.size(); f++) {
                if(!req.getApp().getAppFunctions().contains(f))
                    continue;
                for (int d = 0; d < dcs.size(); d++) {
                    IloNumVar var =  deltaPlaced.get(Triplet.with(r,d,f));
                    IloNumExpr varddSum = cplex.intExpr();
                    for (int dd=0; dd< dcs.size(); dd++) {
                        if (!rd2r_.containsKey(Pair.with(r, dd)))
                            continue;
                        int r_ = rd2r_.get(Pair.with(r,dd));
                        if(deltardPlaced.get(Triplet.with(r_,d,f)).getValue1()==1) {
                            IloNumVar vardd = deltardPlaced.get(Triplet.with(r_, d, f)).getValue0();
                            varddSum = cplex.sum(varddSum, vardd);
                        }
                    }
                    String str = "d("+ r +","+ d +","+ f +")";
                    cplex.addLe(var,varddSum,str+"_delta_sum");
                }
            }
        }
        //Add after cleanup
        for (Map.Entry<IloNumVar, IloConstraint> kv : ddMap.entrySet())
            cplex.add(kv.getValue());
        //Not needed after model cleanup
        ddMap.clear();
        ddMap = null;

        for(Request req: originRequests){
            int rid = req.getId();
            HashMap<Integer,Pair<Integer,Integer>> requestAppPairs = req.getApp().getPairs();
            int activePairs = requestAppPairs.size();
            for (int p=1; p< activePairs-1; p++) { //iterate over function pairs except for zero functions
                for (Map.Entry<Pair<Integer, Integer>, Link> kv : links.entrySet()) {
                    IloNumExpr ysum = cplex.intExpr();
                    Pair<Integer, Integer> link = kv.getKey();
                    for(int dd=0;dd< dcs.size();dd++){
                        if(!rd2r_.containsKey(Pair.with(rid,dd))) //only for requests originating in those dcs
                            continue;
                        int r2_ = rd2r_.get(Pair.with(rid,dd));
                        if(ytrafficdPlaced.containsKey(Triplet.with(r2_,p,link))) {
                            IloNumExpr var = ytrafficdPlaced.get(Triplet.with(r2_, p, link));
                            ysum = cplex.sum(ysum,var);
                        }
                    }
                    if ((kv.getValue()).getBw() > 0) { //if BW 0
                        if(ytrafficPlaced.containsKey(Triplet.with(rid, p, link))) {
                            IloNumExpr yvar = ytrafficPlaced.get(Triplet.with(rid, p, link));
                            String str = tripletYToStr(Triplet.with(rid, p, link));
                            cplex.addLe(yvar, ysum, "yt_sum" + str); //y <= sum(yy)
                        }
                    }
                }
            }
        }
    }

    private static String tripletYToStr(Triplet triplet){
        Pair p = (Pair) triplet.getValue2();
        return "("+ triplet.getValue0() +","+ triplet.getValue1() +
                ",{"+ p.getValue0() +","+ p.getValue1() +"})";
    }

    public static String quatretYToStr(Quartet quatret){
        Pair p = (Pair) quatret.getValue3();
        return "("+ quatret.getValue0() +","+ quatret.getValue1() +","+
                quatret.getValue2() +
                ",{"+ p.getValue0() +","+ p.getValue1() +"})";
    }


    /**
     * Count processing cost of function in each dc
     */
    private void addProcDelayConst(){
        int numOfFunctions = functions.size();
        for (int d=0; d< dcs.size(); d++){
            for (int f=1; f<= numOfFunctions; f++){
                int c = dcs.get(d).getProcPower()* functions.get(f).getProcSize();
                procDelayConst.put(Pair.with(d,f),c);
            }
        }
    }

    private void addConstraints(List<Request> originRequests) throws IloException {
        addBwConstraints(originRequests);
        addCapacityConstrains();
        addPlaceAtUserLocConstrains();
        addNonSplitModeConstrains();

    }

    private void addPlaceAtUserLocConstrains() throws IloException {
        if (!placeAtUserLoc)
            return;
        for (Map.Entry<Triplet<Integer,Integer,Integer>,Pair<IloNumVar,Integer>> kv : deltardPlaced.entrySet()) {
            Triplet<Integer,Integer,Integer> r_df = kv.getKey();
            Pair<IloNumVar,Integer> df = kv.getValue();
            int r_ = r_df.getValue0();
            int userLoc = r_2rd.get(r_).getValue1();
            int fLoc = r_df.getValue1();
            IloNumVar deltad = df.getValue0();
            if (userLoc != fLoc)
                cplex.addEq(deltad, 0, "placeAtUserLoc_" + deltad.getName());
        }
        //can add constrains of deltaPlaced if too slow (future rloc == d)
    }

    private void addNonSplitModeConstrains() throws IloException {
        if (!nonSplitMode)
            return;
        for (Map.Entry<Integer,Pair<Integer,Integer>> kv : r_2rd.entrySet()){
            int r_ = kv.getKey();
            Pair<Integer,Integer> rd = kv.getValue();
            Request req = reqMap.get(rd.getValue0());
            for( int d : dcs.keySet()){
                if(deltardPlaced.containsKey(Triplet.with(r_, d, req.getApp().getAppFunctions().get(0)))){
                    for(Pair<Integer, Integer> fg : getAllPairs(req.getApp().getAppFunctions())) {
                        IloNumVar del1 = deltardPlaced.get(Triplet.with(r_, d, fg.getValue0())).getValue0();
                        IloNumVar del2 = deltardPlaced.get(Triplet.with(r_, d, fg.getValue1())).getValue0();
                        cplex.addEq(del1, del2, "nonSplitMode_" + rd.getValue0() + "_" + d + "_" + fg.getValue0() + "_" + fg.getValue1());
                    }
                }
            }
        }
    }

    private Set<Pair<Integer, Integer>> getAllPairs(List<Integer> integers){
        Set<Pair<Integer, Integer>> uniquePairs = new HashSet<>();

        for (int i = 0; i < integers.size(); i++) {
            for (int j = i + 1; j < integers.size(); j++) {
                Pair pair = new Pair(integers.get(i), integers.get(j));
                uniquePairs.add(pair);
            }
        }
        return uniquePairs;

    }
    private void addBwConstraints(List<Request> originRequests) throws IloException {
        //for each link
        for (Map.Entry<Pair<Integer, Integer>, Link> kv : links.entrySet()) {
            Pair<Integer, Integer> link = kv.getKey();
            IloNumExpr lhs = cplex.intExpr();
            for (Request req : originRequests) {
                HashMap<Integer, Pair<Integer, Integer>> requestAppPairs = req.getApp().getPairs();
                int r = req.getId();
                if (requestAppPairs == null)
                    throw new IllegalArgumentException("Can't find such request ID");
                if ((kv.getValue()).getBw() > 0) { //if BW is 0, all ytraffic are 0
                    for (int p = 0; p < requestAppPairs.size(); p++) { //iterate over function pairs
                        if (ytrafficPlaced.containsKey(Triplet.with(r, p, link)))
                            lhs = cplex.sum(lhs, ytrafficPlaced.get(Triplet.with(r, p, link)));
                    }
                }
            }
            // Add the constraint lhs <= BW
            IloNumExpr term = cplex.constant(( kv.getValue()).getBw());
            String ctName = "BW_link_" + link.getValue0() + "_" + link.getValue1();
            cplex.addLe(lhs, term, ctName);
        }
    }

    private void addCapacityConstrains() throws IloException {
        int numOfFunctions = functions.size();
        for (int d = 0; d < dcs.size(); d++) {
            IloNumExpr lhs = cplex.constant(0);
            for (int r: reqIDs.keySet() ) {
                for (int f = 1; f <= numOfFunctions; f++) {
                    if(deltaPlaced.containsKey(Triplet.with(r, d, f))) //check if var for this function created
                        lhs = cplex.sum(lhs, cplex.prod(deltaPlaced.get(Triplet.with(r, d, f)), functions.get(f).getProcSize()));
                }
            }
            // Add the constraint lhs <= Capacity
            IloNumExpr term = cplex.constant(1);
            String ctName = "Capacity_DC_" + d;
            term = cplex.prod(term, dcs.get(d).getCapacity());
            cplex.addLe(lhs, term,ctName);
        }
    }

    private IloNumExpr calculateCr(int r_, int pairID) throws IloException{
        IloNumExpr Cr = cplex.constant(0);
        Pair<Integer,Integer> link;
        for (Map.Entry<Pair<Integer,Integer>, Link> kvl : links.entrySet()){
            link = kvl.getKey();
            IloNumExpr c = cplex.prod(ytrafficdPlaced.get(Triplet.with(r_,pairID,link)),kvl.getValue().getDelay());
            Cr = cplex.sum(Cr,c);
        }
        return Cr;
    }

    private void addObjective(List<Request> originRequests) throws IloException{
        Boolean end2endLatency = (Boolean)config.get("end2endLatency");
        if (!end2endLatency)
            addPairwiseObjective(originRequests);
        else
            addEnd2EndObjective(originRequests);
    }


    private void addEnd2EndObjective(List<Request> originRequests) throws IloException
    {
        IloNumExpr Sd = cplex.constant(0);
        for(Request req : requests) {
            int appE2EDemand = 0;
            IloNumExpr e2eDelayVar = cplex.intExpr();
            int rid = req.getId();
            int rLoc = req.getLocation();
            int r_ = rd2r_.get(Pair.with(rid,rLoc));
            double weight = req.getWeight();
            int intWeight = (int)(100*weight);
            HashMap<Integer, Pair<Integer, Integer>> requestAppPairs = req.getApp().getPairs();
            ArrayList<IloNumVar> reqDemands = new ArrayList<>(requestAppPairs.size());
            for(int i=0;i<requestAppPairs.size();i++) {
                reqDemands.add(cplex.intVar(0, 1));
                cplex.eq(reqDemands.get(i),0);
            }

            IloNumExpr totalStateCost = cplex.constant(0);
            IloNumExpr ssum = cplex.intExpr();
            IloNumExpr Cr0 = cplex.intExpr();
            boolean flag0 = true; //from user


            for(int pairID=0;pairID<requestAppPairs.size()-1;pairID++) { //avoid last pair (zero pair)
                Pair<Integer,Integer> pair = requestAppPairs.get(pairID);

                int f0 = pair.getValue0();
                int f1 = pair.getValue1();

                //Cr
                IloNumExpr Cr = calculateCr(r_,pairID);

                if(f0==0) { //no processing cost for zero pair, keep routing cost
                    Cr0 = Cr;
                    continue;
                }
                //last pair
                if(pairID == requestAppPairs.size()-2){
                    Cr0 = calculateCr(r_,requestAppPairs.size()-1);
                    Cr = cplex.sum(Cr,Cr0);
                    for (int d = 0; d < dcs.size(); d++) {
                        int localStateCost = dcs.get(d).getExistingSingleState(rid, f1) * dcs.get(d).getStateCost();
                        if (localStateCost > 0 && deltaPlaced.containsKey(Triplet.with(rid, d, f1)))
                            totalStateCost = cplex.sum(totalStateCost, cplex.prod(localStateCost, deltaPlaced.get(Triplet.with(rid, d, f1))));
                    }
                }
                if (flag0){ //count delay from user
                    flag0=false;
                    Cr = cplex.sum(Cr,Cr0);
                }

                //Cp
                IloNumExpr Cp = cplex.constant(0);
                IloNumExpr del0Sum = cplex.constant(0);
                IloNumExpr del1Sum = cplex.constant(0);
                for (int d = 0; d < dcs.size(); d++) {
                    //state launch cost is e(r,d,f) * cost (d) * delta (r,d,f)
                    int localStateCost = dcs.get(d).getExistingSingleState(rid,f0) * dcs.get(d).getStateCost();
                    if(localStateCost>0 && deltaPlaced.containsKey(Triplet.with(rid,d,f0)))
                        totalStateCost = cplex.sum(totalStateCost,cplex.prod(localStateCost,deltaPlaced.get(Triplet.with(rid,d,f0))));

                    IloNumExpr c0 = cplex.constant(procDelayConst.get(Pair.with(d,f0)));
                    IloNumExpr Cp0;
                    if(deltardPlaced.get(Triplet.with(r_,d,f0)).getValue1()==1) {
                        IloNumVar del0 = deltardPlaced.get(Triplet.with(r_, d, f0)).getValue0();
                        del0Sum = cplex.sum(del0Sum, del0);
                        Cp0 = cplex.prod(c0, del0);
                    }
                    else{ //del0=0
                        Cp0 = cplex.constant(0);
                    }

                    IloNumExpr c1 = cplex.constant(procDelayConst.get(Pair.with(d,f1)));
                    IloNumExpr Cp1;
                    if(deltardPlaced.get(Triplet.with(r_,d,f1)).getValue1()==1) {
                        IloNumVar del1 = deltardPlaced.get(Triplet.with(r_, d, f1)).getValue0();
                        del1Sum = cplex.sum(del1Sum, del1);
                        Cp1 = cplex.prod(c1, del1);
                    }
                    else
                        Cp1 = cplex.constant(0);
                    Cp = cplex.sum(Cp,Cp0);
                    Cp = cplex.sum(Cp,Cp1);
                }
                //single demand
                int pairCT = req.getApp().getSinglePairCT(pairID);
                appE2EDemand += pairCT;
                int bigM = pairCT+10;

                e2eDelayVar = cplex.sum(e2eDelayVar, cplex.sum(Cp,Cr));
                String str = rid +","+ rLoc +","+ pairID +")";
//                IloNumVar s1 = cplex.boolVar("s1("+str);
                IloNumVar s2 = cplex.boolVar("s2("+str);
                IloNumVar s3 = cplex.boolVar("s3("+str);
                IloNumVar s = cplex.boolVar("s("+str);
                String ctName = "single_r"+ rid +"_d"+ rLoc + "_p"+ pairID;

                //s1
//                cplex.addLe(cplex.diff(x,cplex.prod(bigM,cplex.diff(1,s1))),pairCT,"s1_leq_"+ctName);  //x-M(1-s) <= gamma
//                cplex.addGe(cplex.sum(x,cplex.prod(bigM,s1)),pairCT+1,"s1_geq_"+ctName);  //x+Ms >= gamma + 1

                //s2 sum(delta_f )>=1
                cplex.addGe(cplex.sum(del0Sum,cplex.prod(bigM,cplex.diff(1,s2))),1,"s2_geq_"+ctName);  //y+M(1-s) >= 1
                cplex.addLe(cplex.diff(del0Sum,cplex.prod(bigM,s2)),0,"s2_leq_"+ctName);  //y-Ms <= 0

                //s3 sum(delta_g )>=1
                cplex.addGe(cplex.sum(del1Sum,cplex.prod(bigM,cplex.diff(1,s3))),1,"s3_geq_"+ctName);  //y+M(1-s) >= 1
                cplex.addLe(cplex.diff(del1Sum,cplex.prod(bigM,s3)),0,"s3_leq_"+ctName);  //y-Ms <= 0

                //merged s
//                cplex.addLe(s,s1,"merged_s_leq1_"+ctName); // s<= s1
                cplex.addLe(s,s2,"merged_s_leq2_"+ctName); // s<= s2
                cplex.addLe(s,s3,"merged_s_leq3_"+ctName); // s<= s3
                cplex.addGe(s,cplex.sum(s2,cplex.diff(s3,1)),"merged_s_geq_"+ctName); // s >= s2+s3-1

                ssum = cplex.sum(ssum,s);
//                sArray.add(s);
            }

            IloNumVar reqSatisfied = cplex.boolVar("St("+ rid  +","+ rLoc +","+ weight + ")");
            int pairs = req.getApp().getNumOfPairs();
            String str = rid + "_d_"+ rLoc;
            if(pairs<1) //otherwise <=1 already exists
                cplex.addLe(ssum,pairs,"Sd_lim_req_"+str); //S<=|Pa|
            int bigMs = 10; //should be ~max|Pa|
            cplex.addGe(ssum,cplex.diff(pairs,cplex.prod(bigMs,cplex.diff(1,reqSatisfied))),
                    "Ssum_1_req_"+str); //s>=|Pa| - M(1-Sd)
            cplex.addGe(pairs,cplex.sum(ssum,cplex.diff(1,cplex.prod(bigMs,reqSatisfied))),
                    "Ssum_2_req_"+str); // |Pa| >= s + 1 - M* Sd

            cplex.setPriority(reqSatisfied,intWeight*req.getRevenue());

            IloNumExpr wReqSatisfied = cplex.prod(reqSatisfied,weight); // *weight
            IloNumExpr wReqSatRevenue = cplex.prod(wReqSatisfied,req.getRevenue());  // *revenue

            sdMap.put(Pair.with(rid,rLoc),reqSatisfied);
            //For threshold
            if(!sdReqMap.containsKey(rid)) { //if first appearance of this rid
                Sd = cplex.sum(Sd,cplex.diff(wReqSatRevenue,totalStateCost)); // - state cost
                sdReqMap.put(rid, wReqSatisfied);
            }
            else { //if not first appearance, do not count in cost
                Sd = cplex.sum(Sd,wReqSatRevenue);
                sdReqMap.put(rid, cplex.sum(sdReqMap.get(rid), wReqSatisfied));
            }
            cplex.addLe(e2eDelayVar, appE2EDemand, "latency_" + r_);
        }

        for(Request req : originRequests){
            int r = req.getId();
            double totalWeight = req.getTotalWeight();
            if(totalWeight==0)
                throw new IllegalStateException("Total weight =0");
            //subtract weight of requests out of system
            double th = reqTh - (1-totalWeight);
//            th = th<0 ? 0 : th; //if <0, no th
            if (th>0 && Objects.equals(type, "Preallocate")){ //if <0, no th
                IloNumVar z = cplex.boolVar("zth"+ r);
                IloNumExpr sd = sdReqMap.get(r);
                cplex.addGe(sd, cplex.prod(th,z), "th_sum_geq_req" + r); //sum >= th * z
                cplex.addLe(sd, z, "th_sum_leq_req" + r); //sum <= z
            }

        }

        cplex.addMaximize(Sd);
    }

    private void addPairwiseObjective(List<Request> originRequests) throws IloException
    {
        IloNumExpr Sd = cplex.constant(0);
        for (Request req : requests) {
            int rid = req.getId();
            double weight = req.getWeight();
            int rLoc = req.getLocation();
            int intWeight = (int)(100*weight);
            HashMap<Integer, Pair<Integer, Integer>> requestAppPairs = req.getApp().getPairs();
            ArrayList<IloNumVar> reqDemands = new ArrayList<>(requestAppPairs.size());
            for(int i=0;i<requestAppPairs.size();i++) {
                reqDemands.add(cplex.intVar(0, 1));
                cplex.eq(reqDemands.get(i),0);
            }

            totalStateCost = cplex.constant(0);
            IloNumExpr ssum = cplex.intExpr();
            for(int pairID=0;pairID<requestAppPairs.size()-1;pairID++)
                ssum = pairToSubRequests(requestAppPairs, pairID, req, ssum);

            IloNumVar reqSatisfied = cplex.boolVar("St("+ rid +","+ rLoc +","+ weight + ")");
            int pairs = req.getApp().getNumOfPairs();
            String str = rid + "_d_"+ rLoc;
            if(pairs<1) //otherwise <=1 already exists
                cplex.addLe(ssum,pairs,"Sd_lim_req_"+str); //S<=|Pa|
            int bigMs = 10; //should be ~max|Pa|
            cplex.addGe(ssum,cplex.diff(pairs,cplex.prod(bigMs,cplex.diff(1,reqSatisfied))),
                    "Ssum_1_req_"+str); //s>=|Pa| - M(1-Sd)
            cplex.addGe(pairs,cplex.sum(ssum,cplex.diff(1,cplex.prod(bigMs,reqSatisfied))),
                    "Ssum_2_req_"+str); // |Pa| >= s + 1 - M* Sd

            cplex.setPriority(reqSatisfied,intWeight*req.getRevenue());

            IloNumExpr wReqSatisfied = cplex.prod(reqSatisfied,weight); // *weight
            IloNumExpr wReqSatRevenue = cplex.prod(wReqSatisfied,req.getRevenue());  // *revenue


            sdMap.put(Pair.with(rid,rLoc),reqSatisfied);
            //For threshold
            if(!sdReqMap.containsKey(rid)) { //if first appearance of this rid
                Sd = cplex.sum(Sd,cplex.diff(wReqSatRevenue,totalStateCost)); // - state cost
                sdReqMap.put(rid, wReqSatisfied);
            }
            else { //if not first appearance, do not count in cost
                Sd = cplex.sum(Sd,wReqSatRevenue);
                sdReqMap.put(rid, cplex.sum(sdReqMap.get(rid), wReqSatisfied));
            }
        }

        for(Request req : originRequests){
            int r = req.getId();
            double totalWeight = req.getTotalWeight();
            if(totalWeight==0)
                throw new IllegalStateException("Total weight =0");
            //subtract weight of requests out of system
            double th = reqTh - (1-totalWeight);
//            th = th<0 ? 0 : th; //if <0, no th
            if (th>0 && Objects.equals(type, "Preallocate")){ //if <0, no th
                IloNumVar z = cplex.boolVar("zth"+ r);
                IloNumExpr sd = sdReqMap.get(r);
                cplex.addGe(sd, cplex.prod(th,z), "th_sum_geq_req" + r); //sum >= th * z
                cplex.addLe(sd, z, "th_sum_leq_req" + r); //sum <= z
            }

        }
        cplex.addMaximize(Sd);
    }

    private IloNumExpr pairToSubRequests(HashMap<Integer, Pair<Integer, Integer>> requestAppPairs, int pairID, Request req,
                                   IloNumExpr ssum) throws IloException {
        Pair<Integer,Integer> pair = requestAppPairs.get(pairID);
        int rid = req.getId();
        int rLoc = req.getLocation();
        int r_ = rd2r_.get(Pair.with(rid,rLoc));
        int f0 = pair.getValue0();
        int f1 = pair.getValue1();
        //Cr
        IloNumExpr Cr = calculateCr(r_,pairID);
        IloNumExpr Cr0 = cplex.intExpr();
        if(f0==0) //no processing cost for zero pair, keep routing cost
            return Cr;
        //last pair
        if(pairID == requestAppPairs.size()-2){
            Cr0 = calculateCr(r_,requestAppPairs.size()-1);
            Cr = cplex.sum(Cr,Cr0);
            for (int d = 0; d < dcs.size(); d++) {
                int localStateCost = dcs.get(d).getExistingSingleState(rid, f1) * dcs.get(d).getStateCost();
                if (localStateCost > 0 && deltaPlaced.containsKey(Triplet.with(rid, d, f1)))
                    totalStateCost = cplex.sum(totalStateCost, cplex.prod(localStateCost, deltaPlaced.get(Triplet.with(rid, d, f1))));
            }
        }
        if (pairID==1) { //count delay from user
            Cr = cplex.sum(Cr, ssum);
            ssum = cplex.intExpr();
        }
        //Cp
        IloNumExpr Cp = cplex.constant(0);
        IloNumExpr del0Sum = cplex.constant(0);
        IloNumExpr del1Sum = cplex.constant(0);
        for (int d = 0; d < dcs.size(); d++) {
            //state launch cost is e(r,d,f) * cost (d) * delta (r,d,f)
            int localStateCost = dcs.get(d).getExistingSingleState(rid,f0) * dcs.get(d).getStateCost();
            if(localStateCost>0 && deltaPlaced.containsKey(Triplet.with(rid,d,f0)))
                totalStateCost = cplex.sum(totalStateCost,cplex.prod(localStateCost,deltaPlaced.get(Triplet.with(rid,d,f0))));
            IloNumExpr c0 = cplex.constant(procDelayConst.get(Pair.with(d,f0)));
            IloNumExpr Cp0;
            if(deltardPlaced.get(Triplet.with(r_,d,f0)).getValue1()==1) {
                IloNumVar del0 = deltardPlaced.get(Triplet.with(r_, d, f0)).getValue0();
                del0Sum = cplex.sum(del0Sum, del0);
                Cp0 = cplex.prod(c0, del0);
            }
            else //del0=0
                Cp0 = cplex.constant(0);
            IloNumExpr c1 = cplex.constant(procDelayConst.get(Pair.with(d,f1)));
            IloNumExpr Cp1;
            if(deltardPlaced.get(Triplet.with(r_,d,f1)).getValue1()==1) {
                IloNumVar del1 = deltardPlaced.get(Triplet.with(r_, d, f1)).getValue0();
                del1Sum = cplex.sum(del1Sum, del1);
                Cp1 = cplex.prod(c1, del1);
            }
            else
                Cp1 = cplex.constant(0);
            Cp = cplex.sum(Cp,Cp0);
            Cp = cplex.sum(Cp,Cp1);
        }
        //single demand
        int pairCT = req.getApp().getSinglePairCT(pairID);
        int bigM = pairCT+10;

        IloNumExpr x = cplex.sum(Cp,Cr);
        String str = rid +","+ rLoc +","+ pairID +")";
        IloNumVar s1 = cplex.boolVar("s1("+str);
        IloNumVar s2 = cplex.boolVar("s2("+str);
        IloNumVar s3 = cplex.boolVar("s3("+str);
        IloNumVar s = cplex.boolVar("s("+str);
        String ctName = "single_r"+ rid +"_d"+ rLoc +  "_p"+ pairID;

        //s1
        cplex.addLe(cplex.diff(x,cplex.prod(bigM,cplex.diff(1,s1))),pairCT,"s1_leq_"+ctName);  //x-M(1-s) <= gamma
        cplex.addGe(cplex.sum(x,cplex.prod(bigM,s1)),pairCT+1,"s1_geq_"+ctName);  //x+Ms >= gamma + 1

        //s2 sum(delta_f )>=1
        cplex.addGe(cplex.sum(del0Sum,cplex.prod(bigM,cplex.diff(1,s2))),1,"s2_geq_"+ctName);  //y+M(1-s) >= 1
        cplex.addLe(cplex.diff(del0Sum,cplex.prod(bigM,s2)),0,"s2_leq_"+ctName);  //y-Ms <= 0

        //s3 sum(delta_g )>=1
        cplex.addGe(cplex.sum(del1Sum,cplex.prod(bigM,cplex.diff(1,s3))),1,"s3_geq_"+ctName);  //y+M(1-s) >= 1
        cplex.addLe(cplex.diff(del1Sum,cplex.prod(bigM,s3)),0,"s3_leq_"+ctName);  //y-Ms <= 0

        //merged s
        cplex.addLe(s,s1,"merged_s_leq1_"+ctName); // s<= s1
        cplex.addLe(s,s2,"merged_s_leq2_"+ctName); // s<= s2
        cplex.addLe(s,s3,"merged_s_leq3_"+ctName); // s<= s3
        cplex.addGe(s,cplex.sum(s1,cplex.sum(s2,cplex.diff(s3,2))),"merged_s_geq_"+ctName); // s >= s1+s2+s3-2

        return cplex.sum(ssum,s);
    }


    public void solve(boolean estimated) throws IloException
    {
        try{
            cplex.readParam("include/cplex_config.prm");
            int seed = (int)config.get("seed");
            //no time limit
            cplex.setParam(IloCplex.Param.RandomSeed, seed);
            if(logdir.contains("full")) {
                int fullTimeLimit = (int)config.get("fullTimeLimit");
                if (fullTimeLimit>0)
                    cplex.setParam(IloCplex.Param.DetTimeLimit, fullTimeLimit);
                else
                    cplex.setParam(IloCplex.Param.TimeLimit, 1e+75);
                //For speed
                cplex.setParam(IloCplex.Param.MIP.Strategy.VariableSelect,4);
                cplex.setParam(IloCplex.Param.MIP.Strategy.Probe,-1);
            }
            else{
                if(!placeAtUserLoc) {
                    cplex.setParam(IloCplex.Param.Emphasis.MIP, 1);
                    cplex.setParam(IloCplex.Param.MIP.Pool.Intensity, 1);
                }
                if(!estimated){
                    int limitRatio = 5;
                    double timeLimit =  cplex.getParam(IloCplex.Param.TimeLimit);
                    timeLimit /= limitRatio;
                    cplex.setParam(IloCplex.Param.TimeLimit,(int)timeLimit);
                }
            }
            System.out.println("Launch solver");
            gotSolution = cplex.solve();

        } catch (IloException e){
            cplex.getCplexStatus();
        }
    }

    public double getGap() throws IloException {
        if (!gotSolution)
            return 100;
        return cplex.getMIPRelativeGap();
    }



    public void getSolution() throws IloException, IOException {
        int numOfFunctions = functions.size();
        boolean nonSplitMode = (Boolean) config.get("nonSplitMode");
        PrintWriter writer = new PrintWriter(new FileOutputStream(
                new File(this.logdir + "/lp_summary"+"_t"+ time +"_"+type + ".txt"),
                true /* append = true */));
        writer.println("\n*** " + type + " of interval " + time + " ***\n");
        if(debugMode) {
            cplex.writeSolution(this.logdir + "/solution" + "_t" + time + "_" + type + ".lp");
            int numConstraints = cplex.getNrows();
            System.out.println("Number of constraints: " + numConstraints);
            int numVars = cplex.getNcols();
            System.out.println("Number of variables: " + numVars);


        }
        writer.println("Requests by DC:");

        double satSum=0;
        double totalSum=0;
        HashMap<Integer,ArrayList<String>> yreq= new HashMap<>();
        for (int r: reqIDs.keySet())
            yreq.put(r,new ArrayList<>());
        for (IloNumVar y : ytrafficArray){
            if (!gotSolution) {
                System.out.println("No solution found in t=" + time +" - "+type);
                break;
            }
            String str = y.getName();
            if(cplex.getValue(y)>0) {
                String[] strArr = str.split(",");
                int req = Integer.parseInt(strArr[0].substring(2));
                ArrayList<String> l = yreq.get(req);
                l.add(str);
//                yreq.put(req,l);
            }
        }

//        for (int r=0; r< requests.size(); r++){
        for(Request req : requests){
//            Request req = requests.get(r);
            int rid = req.getId();
            int dSrc = req.getLocation();
            int r_ = rd2r_.get(Pair.with(rid,dSrc));
            StringBuilder sb = new StringBuilder();
            if(gotSolution && cplex.getValue(sdMap.get(Pair.with(rid,dSrc)))==1){
                sb.append("\nV");
                satSum+= 100*req.getWeight();
                reqIDs.put(rid,true);
            }
            else {
                sb.append("\nX");
                totalSum += 100*req.getWeight();
            }
            sb.append(" - Request: ").append(rid).append(", location ").append(req.getLocation()).append(", app ").
                    append(req.getAppS()).append(", weight ").append(req.getWeight()).append(", revenue ").
                    append(req.getRevenue());
            writer.println(sb);
            sb.setLength(0);
            for(int i=0;i<req.getApp().getPairs().size();i++){
                Pair<Integer, Integer> pair = req.getApp().getPairs().get(i);
                int pairCT = req.getApp().getSinglePairCT(i);
                sb.append("{").append(pair.getValue0()).append(",").append(pair.getValue1()).append("} <= ").
                        append(pairCT).append(" , ");
                writer.println(sb);
                sb.setLength(0);
            }
            writer.println();
            //print deltas for request
            for (int d=0; d< dcs.size(); d++){
                if (!gotSolution)
                    break;
                int nonSplitDC = -1;
                for( int f : req.getApp().getAppFunctions()){
                    try {
                        IloNumVar var = deltardPlaced.get(Triplet.with(r_, d, f)).getValue0();
                        if(cplex.getValue(var)>0) {
                            StringBuilder sbf = new StringBuilder();
                            sbf.append("d(").append(rid).append(",").append(d).append(",").append(f).append(")");
                            if(nonSplitMode && f < numOfFunctions-2)
                                nonSplitDC = verifyNonSplitResult(d, nonSplitDC);
                            int c = procDelayConst.get(Pair.with(d,f));
                            sb.append(c).append(" X ").append(sbf);
                            writer.println(sb);
                            sb.setLength(0);
                        }
                    } catch (IloCplexModeler.Exception e){
                        continue;
                    }
                }
            }

            for(int p=0; p<=req.getApp().getNumOfPairs()+1;p++){
                if (!gotSolution)
                    break;
                for(int d1=0; d1< dcs.size(); d1++){
                    for(int d2=0; d2< dcs.size(); d2++){
                        Triplet<Integer, Integer, Pair<Integer, Integer>> triplet = Triplet.with(r_, p, Pair.with(d1,d2));
                        Quartet<Integer, Integer, Integer, Pair<Integer, Integer>> quatret = Quartet.with(rid,dSrc, p, Pair.with(d1,d2));
                        IloNumExpr yVar = ytrafficdPlaced.get(triplet);
                        if(yVar!=null && cplex.getValue(yVar)>0) {
                            int linkDelay = links.get(Pair.with(d1, d2)).getDelay();
                            sb.append(linkDelay).append(" X yy").append(quatretYToStr(quatret));
                            writer.println(sb);
                            sb.setLength(0);
                        }
                    }
                }
            }
        }

        totalSum /= 100;
        satSum /= 100;
        totalSum += satSum;
        writer.println("\nSatisfied requests: " + satSum + " / " + totalSum);
        writer.close();

    }

    private int verifyNonSplitResult(int d, int nonSplitDC){
        if (nonSplitDC==-1)
            return d;
        if(nonSplitDC != d)
            throw new IllegalStateException("Non split mode not accomplished");
        return nonSplitDC;
    }

    public void cleanup()
    {
        try {
            cplex.clearModel();
        }catch (IloException e){
            e.printStackTrace();
        }
        cplex.end();
        System.gc();
    }


    /**
     * Updates capacities for links and dcs which are unused in current request
     * This occurs when one DC is correctly estimated, and others can be released
     * @param request request
     * @param srcDC real src DC of request. If -1 release all resources.
     */
    private void releaseUnusedResources(Request request, int srcDC) throws IloException {
        int rid = request.getId();
        if(srcDC==-1){ //release all request resources
            //DC Capacity
            for(int d=0;d<dcs.size();d++) { //For each DC
                for (int f : request.getApp().getAppFunctions()) { //for each function in request
                    if (cplex.getValue(deltaPlaced.get(Triplet.with(rid, d, f))) == 1){
                        dcs.get(d).setCapacity(dcs.get(d).getCapacity() + functions.get(f).getProcSize()); //increase available capacity
                    }
                }
            }

            //BW
            for (Map.Entry<Pair<Integer, Integer>, Link> kv : links.entrySet()) {
                Pair<Integer, Integer> l = kv.getKey();
                for (int p : request.getApp().getPairs().keySet()) {
                    if (cplex.getValue(ytrafficPlaced.get(Triplet.with(rid,p,l)))==1)
                        links.get(l).updateBW(1);
                }
            }
            return;
        }

        //Partial release (only unused)
        //DC
        int rActualUsed = rd2r_.get(Pair.with(rid, srcDC));
        List<Integer> optionalDCs = new ArrayList<>(dcs.keySet());
        optionalDCs.remove((Integer) srcDC);
        for(int d=0;d<dcs.size();d++){
            for (int f : request.getApp().getAppFunctions()) { //for each function in request
//                for(int dReq=0;dReq<dcs.size();dReq++){  //src of request
                for(int dReq : optionalDCs){
                    if(rd2r_.get(Pair.with(rid, dReq))==null) //request from this location not expected
                        continue;
                    int r_ = rd2r_.get(Pair.with(rid, dReq));
                    if(rActualUsed == r_)
                        throw new IllegalStateException("Attempting to remove actual request");
                    if(deltardPlaced.get(Triplet.with(rActualUsed, d, f)).getValue1()==0 || deltardPlaced.get(Triplet.with(r_, d, f)).getValue1()==0)
                        continue;
                    if (cplex.getValue(deltardPlaced.get(Triplet.with(rActualUsed, d, f)).getValue0()) == 1) { //real request uses this
                        continue;
                    }
                    else if (cplex.getValue(deltardPlaced.get(Triplet.with(r_, d, f)).getValue0()) == 1) { //if unused function uses this DC
                        dcs.get(d).setCapacity(dcs.get(d).getCapacity() + functions.get(f).getProcSize()); //increase available capacity
                        break; //next function
                    }
                }
            }
        }

        //handle traffic
        for (Map.Entry<Pair<Integer, Integer>, Link> kv : links.entrySet()) {
            Pair<Integer, Integer> l = kv.getKey();
            for (int p : requests.get(rActualUsed).getApp().getPairs().keySet()) {
                if(cplex.getValue(ytrafficdPlaced.get(Triplet.with(rActualUsed,p,l)))==1)//real request uses this
                    continue;
                for(int dReq=0;dReq<dcs.size();dReq++) {
                    if(rd2r_.get(Pair.with(rid, dReq))==null) //request from this location not expected
                        continue;
                    int r = rd2r_.get(Pair.with(rid, dReq));
                    if (cplex.getValue(ytrafficdPlaced.get(Triplet.with(r,p,l)))==1){
                        links.get(l).updateBW(1); //increase available capacity
                        if(links.get(l).getBw()<0)
                            throw new IllegalStateException("BW<0");
                        break; //next pair
                    }
                }
            }
        }
    }



    public void analyzeUtilizedCapacities()
            throws IloException, FileNotFoundException {
        File placedAppsFile = new File(this.logdir+"/dump/placedApps.csv");
        File placedFuncFile = new File(this.logdir+"/dump/placedFunctions.csv");
        File dcUtilFile = new File(this.logdir+"/dump/dcUtilization.csv");
        File linkUtilFile = new File(this.logdir+"/dump/linkUtilization.csv");
        PrintWriter writer = new PrintWriter(new FileOutputStream(
                new File(this.logdir+"/capacities.log"),
                true),true);
        PrintWriter placedAppWriter = new PrintWriter(new FileOutputStream(placedAppsFile, true),true);
        PrintWriter placedFuncWriter = new PrintWriter(new FileOutputStream(placedFuncFile, true),true);
        PrintWriter dcUtilWriter = new PrintWriter(new FileOutputStream(dcUtilFile, true),true);
        PrintWriter linkUtilWriter = new PrintWriter(new FileOutputStream(linkUtilFile, true),true);

        //Initialize map
        if(Objects.equals(type, "Preallocate")) {
            for (int d = 0; d < dcs.size(); d++) {
                for(Map.Entry<Integer, App> kv : apps.entrySet()){
                    int appID = kv.getKey();
                    reservedApps.put(Pair.with(d, appID), new ArrayList<>());
                }
            }
        }

        if(placedAppsFile.length()==0)
            placedAppWriter.println("time,type,reqID,dc,appID,size,revenue,predicted,cost,weight,redirected");
        if(placedFuncFile.length()==0)
            placedFuncWriter.println("time,type,reqID,funcID,dc,size");
        if(dcUtilFile.length()==0)
            dcUtilWriter.println("time,type,dc,placed,capacity,utilization");
        if(linkUtilFile.length()==0)
            linkUtilWriter.println("time,type,src,dst,placed,capacity,utilization");

        writer.println();
        writer.println("---------- " + type +" Demand Analysis Interval "+ time + " ----------");
        writer.println();


        //DC capacity
        int[] placedFuncs = new int[dcs.size()];
        Arrays.fill(placedFuncs,0);
        HashMap<Integer,Integer> paidForState = new HashMap<>(); //<r,paid>
        for(Map.Entry<Triplet<Integer,Integer,Integer>,IloNumVar> kv : deltaPlaced.entrySet()){
            Triplet<Integer,Integer,Integer> rdf = kv.getKey();
            int r = rdf.getValue0();
            int d = rdf.getValue1();
            int f = rdf.getValue2();
            if(gotSolution && cplex.getValue(kv.getValue())==1){ //if delta is '1' (placed)
                //Sum how much paid for states for this request
                if(paidForState.containsKey(r)){
                    int paid = paidForState.get(r);
                    paid += dcs.get(d).getExistingSingleState(r,f)*dcs.get(d).getStateCost();
                    paidForState.put(r,paid);
                }
                else
                    paidForState.put(r,dcs.get(d).getExistingSingleState(r,f)*dcs.get(d).getStateCost());
                dcs.get(d).setExistingSingleState(r,f,true);

                placedFuncWriter.println(time +","+ type +","+ rdf.getValue0() +","+
                        f +","+ d +","+ functions.get(f).getProcSize());
                placedFuncs[d]+=functions.get(f).getProcSize();
            }
            else
                dcs.get(d).setExistingSingleState(r,f,false);
        }
        List<Integer> placedList = new ArrayList<>();
        for (Request req : requests) {
            if (!gotSolution)
                break;
            int rid = req.getId();
            int dSrc =req.getLocation();
            if(cplex.getValue(sdMap.get(Pair.with(rid,dSrc)))==1){
                numOfAllocated++;
                placedList.add(rid);
                int redirected = 0;
                placedAppWriter.println(time + "," + type + "," + rid + "," + dSrc
                        + "," + req.getAppS() + "," + req.getApp().getTotalSize() + "," +
                        req.getRevenue() + ","+ "0"+ ","+ paidForState.get(rid) + ","+ req.getWeight() + "," + redirected);
                if(Objects.equals(type, "Preallocate")) {
                    int a = req.getApp().getId();
                    List<Request> curReserved = reservedApps.get(Pair.with(dSrc, a));
                    curReserved.add(req);
//                    reservedApps.put(Pair.with(dSrc, a), curReserved);
                }
            }
        }
        HashSet<Integer> uniqueIds= new HashSet<>(placedList);
        writer.println("Placed Requests: " + uniqueIds.size() + "/" + reqIDs.size() + " - Passed: " +
                placedList.size() + "/" + requests.size());
        placedList = new ArrayList<>(uniqueIds);
        Collections.sort(placedList);
        for (Integer integer : placedList)
            writer.print(integer + ", ");
        writer.println();


        writer.println("DC Utilization:");
        for (int d=0; d< dcs.size();d++) {
            BigDecimal capUsed = new BigDecimal(dcs.get(d).getOriginalCapacity()- dcs.get(d).getCapacity());
            BigDecimal c = new BigDecimal(dcs.get(d).getOriginalCapacity());
            BigDecimal del = new BigDecimal(placedFuncs[d]);
            del = del.add(capUsed);
            BigDecimal utilization = del.divide(c,2, RoundingMode.HALF_UP);
            utilization = utilization.multiply(new BigDecimal(100));
            writer.println("DC " + d + ": " + del + "/"+c + " (" +
                    utilization.intValue() + "%)");
            dcUtilWriter.println((time) +","+ type +","+d+","+del+
                    ","+c+","+utilization.intValue());

            //Updated non-utilized capacity
            dcs.get(d).setCapacity(dcs.get(d).getCapacity()-placedFuncs[d]);
        }


        //Link Capacity
        writer.println();
        writer.println("Link Utilization:");
        int[][] usedLinks = new int[dcs.size()][dcs.size()];
        int[][] bw = new int[dcs.size()][dcs.size()];
        for(int[] row : usedLinks){
            Arrays.fill(row,0);
        }
        for(Map.Entry<Triplet<Integer,Integer,Pair>,IloNumExpr> kv : ytrafficPlaced.entrySet()){
            if (!gotSolution)
                break;
            Triplet<Integer,Integer,Pair> rpl = kv.getKey();

            if(cplex.getValue(kv.getValue())==1){ //if y is '1' (placed)
                Pair<Integer,Integer> link = rpl.getValue2();
                usedLinks[link.getValue0()][link.getValue1()]++;
            }
        }
        for(Map.Entry<Pair<Integer,Integer>, Link> kv : links.entrySet()){
            Pair<Integer,Integer> pair = kv.getKey();
            Link link = (Link) kv.getValue();
            bw[pair.getValue0()][pair.getValue1()] = link.getBw();
        }

        for(int i=0; i< dcs.size();i++){
            for (int j=0; j< dcs.size(); j++){
                if(bw[i][j] > 0){
                    BigDecimal diff = new BigDecimal(links.get(Pair.with(i,j)).getBw() - bw[i][j]);
                    BigDecimal c = new BigDecimal(links.get(Pair.with(i,j)).getBw());
                    BigDecimal y = new BigDecimal(usedLinks[i][j]);
                    y = y.add(diff);
                    BigDecimal utilization = y.divide(c,2, RoundingMode.HALF_UP);
                    utilization = utilization.multiply(new BigDecimal(100));
                    writer.println("Link {" + i +","+j+
                            "}: " + y + "/"+c + " (" +
                            utilization.intValue() + "%)");
                    linkUtilWriter.println((time) +","+ type +","+i +","+j +
                            ","+y +","+c+","+utilization.intValue());
                    links.get(Pair.with(i,j)).updateBW(-usedLinks[i][j]);
                    if(links.get(Pair.with(i,j)).getBw()<0)
                        throw new IllegalStateException("BW<0");
                }
            }
        }
    }

    public void updateDCStateCosts() throws FileNotFoundException {
        Path dump = fileHandler.getDumpdir();
        File file = new File(dump + "/stateCost.txt");
        PrintWriter fileWriter = new PrintWriter(new FileOutputStream(file, true ),true);
        if(file.length()==0)
            fileWriter.println("time,dc,cost");

        int dynamicStateCostStep = (int) config.get("dynamicStateCostStep");
        double capThreshold = 0.95;
        for (int d=0; d< dcs.size();d++) {
            double utilizedCapacity = (initialCapacities[d] - dcs.get(d).getCapacity()) / (double)initialCapacities[d];
            int stateCost = dcs.get(d).getStateCost();
            int newStateCost = stateCost;
            if (utilizedCapacity >= capThreshold ) {
                newStateCost = stateCost + dynamicStateCostStep;
                dcs.get(d).setStateCost(newStateCost);
            }
            else if ((utilizedCapacity <= 1-capThreshold) &&  stateCost >= dynamicStateCostStep){
                newStateCost = stateCost - dynamicStateCostStep;
                dcs.get(d).setStateCost(newStateCost);
            }
            fileWriter.println(time +","+ d +","+ newStateCost);
        }
    }

    public HashMap<Integer, Boolean> getReqIDs() {
        return reqIDs;
    }

    private void addToCurrentRequests(HashMap<Pair<Integer,Integer>,List<Request>> currentRequests, Request request){
        int d = request.getLocation();
        int a = request.getApp().getId();
        List<Request> list = null;
        if (currentRequests.containsKey(Pair.with(d,a)))
            list = currentRequests.get(Pair.with(d,a));
        else
            list = new ArrayList<>();
        list.add(request);
        currentRequests.put(Pair.with(d,a),list);
    }

    private void removeFromRemainingReserved(int r, int dc, int a){
        for(int d=0;d< dcs.size();d++){
            if (d==dc)
                continue;
            List<Request> requests = reservedApps.get(Pair.with(d,a));
            if(requests.contains(r)) {
                requests.remove((Integer) r);
                reservedApps.put(Pair.with(d,a),requests);
            }
        }
    }


    public List<Request> analyzeCurrentDemand(List<Request> actualRequests) throws IloException, FileNotFoundException {
        List<Integer> unusedRequests = new ArrayList<>();
        List<Integer> curRequestsIDs = new ArrayList<>();
        List<Request> requestsToServe = new ArrayList<>();
        HashMap<Pair<Integer,Integer>,List<Request>> currentRequests = new HashMap<>(); //<d,a>
        List<Integer> redirectedAllocations = new ArrayList<>();
        type = "Evaluated";
        PrintWriter writer = new PrintWriter(new FileOutputStream(new File(this.logdir+"/capacities.log"), true ),true);
        File placedAppsFile = new File(this.logdir+"/dump/placedApps.csv");
        PrintWriter placedAppWriter = new PrintWriter(new FileOutputStream(placedAppsFile, true),true);
        writer.println();
        writer.println("---------- Adjust Demand vs. Prealloc "+ time + "----------");
        writer.println();
        writer.println("Served requests:");
        writer.println();

        //Actual request to ordered list by revenue
        List<Request> requestList = new ArrayList<>(actualRequests);
        requestList.sort(Comparator.comparingInt(Request::getRevenue));
        Collections.reverse(requestList);
        int newReq = collectCurrentRequests(currentRequests, actualRequests, unusedRequests, curRequestsIDs, redirectedAllocations);
        int expired = removeExpiredRequests(curRequestsIDs, unusedRequests);
        sortReservedAppsByBenefit();
        int[] counted = handleCurrentRequests(currentRequests, unusedRequests, writer, placedAppWriter, requestsToServe,
                redirectedAllocations);
        int repurposed = counted[0];
        int missed = counted[1];
        int redirected = redirectedAllocations.size();


        writer.println();
        writer.println();
        writer.println("Unused reservations:");
        writer.println();

        List<Integer> unusedList = new ArrayList<>();
        //after all new requests used unused reservation, free remaining unused
        for(Map.Entry<Pair<Integer,Integer>, List<Request>> kv : reservedApps.entrySet()){
            List<Request> reservedList = kv.getValue();
            for (Request request : reservedList) {
                if(unusedRequests.contains(request.getId())) {
                    removeFromRemainingReserved(request.getId(),-1,request.getApp().getId());
                    unusedRequests.remove((Integer) request.getId());
                    releaseUnusedResources(request,-1);
                    unusedList.add(request.getId());
                }
            }
        }
        Collections.sort(unusedList);
        for (Integer integer : unusedList)
            writer.print(integer + ", ");

        writer.println();
        writer.println();
        writer.println("To serve:");
        writer.println();
        List<Integer> toServe =  requestsToServe.stream().map(Request::getId).collect(Collectors.toList());
        Collections.sort(toServe);
        for (Integer integer : toServe)
            writer.print(integer + ", ");
        writer.println();

        int reqsPlaced = Collections.frequency(reqIDs.values(), true);
        int reqsServed = actualRequests.size() - requestsToServe.size();
        writer.println();
        writer.println("Requests Served/Placed: " + reqsServed + "/" + reqsPlaced +
                "     |     Repurposed: " + repurposed + "\n, redirected: " + redirected + ", not reused: " + missed +
                ", expired: " + expired + ", new: " + newReq);
        writer.println();
        return requestsToServe;

    }

    /**
     * for each actual request
     * list all: 1)unused reservation 2)Retries (missed and new)
     *
     * @param actualRequests
     * @param unusedRequests
     * @param curRequestsIDs
     * @param redirectedAllocations
     * @throws IloException
     */
    private int collectCurrentRequests
            (HashMap<Pair<Integer,Integer>,List<Request>> currentRequests, List<Request> actualRequests,
             List<Integer> unusedRequests, List<Integer> curRequestsIDs,
             List<Integer> redirectedAllocations) throws IloException {
        int newReq = 0;
        for(Request request: actualRequests) {
            int r = request.getId();
            int d = request.getLocation();
            curRequestsIDs.add(r);
            if (reqIDs.containsKey(r)) { //if request previously existed
                if (reqIDs.get(r)) { //request placed
                    if (!sdMap.containsKey(Pair.with(r, d)) || cplex.getValue(sdMap.get(Pair.with(r, d))) == 0) { //If not predicted location
                        addToCurrentRequests(currentRequests,request);
                        if(useMispredAlloc(request))
                            redirectedAllocations.add(r);
                        else
                            unusedRequests.add(r);
                    }
                    else { //successfully predicted
                        addToCurrentRequests(currentRequests, request);
//                        int contains = sdMap.containsKey(Pair.with(r, d)) ? 1 : 0;
//                        if (contains != cplex.getValue(sdMap.get(Pair.with(r, d))))
//                            throw new IllegalStateException("sdMap not matches allocation"); //test to see if can remove one of them from if
                    }
                }
                else //previously denied, try again
                    addToCurrentRequests(currentRequests,request);
            }
            else { //new request
                addToCurrentRequests(currentRequests, request);
                newReq++;
            }
        }
        return newReq;
    }
    private int removeExpiredRequests(List<Integer> curRequestsIDs, List<Integer> unusedRequests){
        int expired = 0;
        for(int r:reqIDs.keySet()){
            if(!curRequestsIDs.contains(r)){ //no longer exists
                if (reqIDs.get(r)) {
                    expired++;
                    unusedRequests.add(r);
                    for(int d=0;d<dcs.size();d++)
                        dcs.get(d).removeStates(r,functions.keySet());
                }
            }
        }
        return expired;
    }

    private int[] handleCurrentRequests(HashMap<Pair<Integer,Integer>,List<Request>> currentRequests,
                                       List<Integer> unusedRequests, PrintWriter writer, PrintWriter placedAppWriter,
                                       List<Request> requestsToServe,
                                      List<Integer> redirectedAllocations) throws IloException {
        int repurposed = 0;
        int missed = 0;
        for(Map.Entry<Pair<Integer,Integer>,List<Request>> kv : currentRequests.entrySet()){
            Pair<Integer,Integer> da = kv.getKey();
            int d = da.getValue0();
            int a = da.getValue1();
            List<Request> curReqList = kv.getValue();
            //sort requests for this <d,a> by revenue
            curReqList.sort(Comparator.comparingInt(Request::getRevenue));
            Collections.reverse(curReqList);
            List<Request> reservedAppsList = reservedApps.get(Pair.with(d,a));
            //list of IDs in this locations
            List<Integer> reservedIdList = reservedAppsList.stream().map(Request::getId).collect(Collectors.toList());
            List<Integer> unusedIDs = new ArrayList<>(reservedIdList);
            unusedIDs.retainAll(unusedRequests); //retain only intersection between idList in this <d,a> and unused
            for (Request req : curReqList) { //iterate over actual requests
                int rid = req.getId();
                int redirected = 0;
                if (reservedIdList.contains(rid)) { //predicted
                    releaseUnusedResources(req, d);
                    removeFromRemainingReserved(rid, d, a);
                    int predicted = 1;
                    int cost = 0;
                    if (redirectedAllocations.contains(rid))
                        redirected = 1;
                    placedAppWriter.println(time + "," + type + "," + rid + "," + d
                            + "," + req.getAppS() + "," + req.getApp().getTotalSize() + "," +
                            req.getRevenue() + "," + predicted + "," + cost + "," + req.getWeight() +"," +redirected);
                    writer.print(rid + ", ");
                }
                else if (unusedIDs.size() > 0) { //not in reserved IDs, but there are unused reservations
                    for (int i = 0; i < reservedAppsList.size(); i++) { //find unused reservations
                        Request resRequest = reservedAppsList.get(i);
                        if (unusedIDs.contains(resRequest.getId())) {  //if IDs match
                            int predicted=0;
                            int cost = 0;
                            double weight = 1;
                            repurposed++;
                            releaseUnusedResources(resRequest, d);
                            unusedRequests.remove((Integer) resRequest.getId());
                            removeFromRemainingReserved(resRequest.getId(), d, a);
                            placedAppWriter.println(time + "," + type + "," + rid + "," + d
                                    + "," + req.getAppS() + "," + req.getApp().getTotalSize() + "," +
                                    req.getRevenue() + "," + predicted + "," + cost +"," + weight +"," + redirected);
                            writer.print(rid + "(" + resRequest.getId() + "), ");
                            reservedAppsList.add(req);
                            unusedIDs.remove((Integer) resRequest.getId());
                            break;
                        }
                    }
                } else {
                    requestsToServe.add(req);
                    missed++;
                }
            }
        }
        return new int[]{repurposed, missed};
    }

    private void sortReservedAppsByBenefit(){
        for(Map.Entry<Pair<Integer,Integer>, List<Request>> kv : reservedApps.entrySet()){
            List<Request> requests = kv.getValue();
            if(requests.size()>1){
//                Pair<Integer,Integer> k = kv.getKey();
                requests.sort(Comparator.comparingInt(Request::getRevenue));
                Collections.reverse(requests);
//                reservedApps.put(k,requests);
            }
        }
    }

    private boolean useMispredAlloc(Request request) throws IloException {
        if (!checkBrokenChains)
            return false;
        int r = request.getId();
        int dUser = request.getLocation();
        boolean safeInAllPairs = false;
        App app = request.getApp();
        List<Integer> neighbors = dcs.get(dUser).getNeigbors();
        neighbors.remove((Integer) dUser);
        List<Integer> funcLocations = new ArrayList<>();
        funcLocations.add(dUser); //zero func
        for(int dAlloc : neighbors){
            boolean chainBroke = false;
//            if(sdMap.containsKey(Pair.with(r, dAlloc))){
            if(sdMap.containsKey(Pair.with(r, dAlloc)) && cplex.getValue(sdMap.get(Pair.with(r,dAlloc)))==1){
                int r_ = rd2r_.get(Pair.with(r,dAlloc));
                for (int p=0; p < app.getPairs().size(); p++){
                    if (p>1 && !safeInAllPairs)
                        break;
                    int f = app.getPairs().get(p).getValue0();
                    int g = app.getPairs().get(p).getValue1();
                    if(f==0)
                        continue;
                    if(g==0 && safeInAllPairs){
                        funcLocations.add(dUser);
                        return redirectAllocationIfPossible(request, funcLocations, dAlloc);
                    }
                    int fLoc = -1;
                    int gLoc = -1;
                    List<Integer> dAllocNeighbors = new ArrayList<>(dcs.get(dAlloc).getNeigbors());
                    //keep intersection between user loc neighbors and dAlloc neighbors
                    dAllocNeighbors.retainAll(neighbors);
                    for(int d : dAllocNeighbors) {
                        if (!deltardPlaced.containsKey(Triplet.with(r_, d, f)) ||
                                !deltardPlaced.containsKey(Triplet.with(r_, d, g)))
                            continue;
                        if (fLoc == -1 && cplex.getValue(deltardPlaced.get(Triplet.with(r_, d, f)).getValue0()) == 1) {
                            fLoc = d;
                        }
                        if (gLoc == -1 && cplex.getValue(deltardPlaced.get(Triplet.with(r_, d, g)).getValue0()) == 1) {
                            gLoc = d;
                        }
                        if (fLoc != -1 && gLoc != -1){
                            int linkDelay = 0;
                            int delayFromUser = 0;
                            int delayToUser = 0;
                            if (fLoc != gLoc) {
                                if (cplex.getValue(ytrafficdPlaced.get(Triplet.with(r_, p, Pair.with(fLoc, gLoc)))) == 0)
                                    throw new IllegalStateException("Link not allocated: (" + fLoc + ", " + gLoc + ")");
                                linkDelay = links.get(Pair.with(fLoc, gLoc)).getDelay();
                            }
                            if(p==1)
                                delayFromUser = getDelayOnLinkIfExists(dUser, fLoc);
                            else if (p==app.getPairs().size()-2)
                                delayToUser = getDelayOnLinkIfExists(gLoc, dUser);
                            int fDelay = procDelayConst.get(Pair.with(fLoc, f));
                            int gDelay = procDelayConst.get(Pair.with(gLoc, f));
                            int pairCT = app.getSinglePairCT(p);
                            int totalDelay = delayFromUser + fDelay + linkDelay + gDelay + delayToUser;
                            if (totalDelay > pairCT) {
                                chainBroke = true;
                                safeInAllPairs = false;
                            }
                            else {
                                safeInAllPairs = true;
                                funcLocations.add(fLoc);
                            }
                            break;
                        }
                    }
                    if(chainBroke)
                        break;
                }
            }
        }
        return false;
    }

    private Request getNeighborReq(Request currentReq, int dNeighbor){
        List<Request> curReserved = reservedApps.get(Pair.with(dNeighbor, currentReq.getApp().getId()));
        for (Request request : curReserved){
            if (request.getId() == currentReq.getId())
                return request;
        }
        throw new IllegalStateException("Request not found in DC");
    }

    private boolean redirectAllocationIfPossible(Request request, List<Integer> funcLocations, int dNeighbor){
        Request neighborReq = getNeighborReq(request, dNeighbor);
        if(!updateLinks(funcLocations, dNeighbor))
            return false;
        redirectNeighborAllocationToCurrentLocation(request, neighborReq);

        return true;
    }

    private boolean updateLinks(List<Integer> funcLocations, int dNeighbor){
        int userLoc = funcLocations.get(0);
        int firstFuncLoc = funcLocations.get(1);
        int lastFuncLoc = funcLocations.get(funcLocations.size()-2);
        Link firstOldLink;
        Link lastOldLink;
        if (userLoc==firstFuncLoc && userLoc==lastFuncLoc) {
//            firstOldLink = links.get(Pair.with(dNeighbor, firstFuncLoc));
//            lastOldLink = links.get(Pair.with(lastFuncLoc, dNeighbor));
//            firstOldLink.updateBW(-1);
//            lastOldLink.updateBW(-1);
            return true;
        }
        return updateRedirectedLinkBW(userLoc, firstFuncLoc) && updateRedirectedLinkBW(lastFuncLoc, userLoc);
    }

    private boolean updateRedirectedLinkBW(int d1, int d2){
        if(d1==d2)
            return true;
        Link link = links.get(Pair.with(d1, d2));
        if (link.getBw()>0) {
            link.updateBW(-1);
            return true;
        }
        return false;
    }

    private void redirectNeighborAllocationToCurrentLocation(Request userReq, Request neighborReq){
        int requestID = userReq.getId();
        List<Request> neighborReserved = reservedApps.get(Pair.with(neighborReq.getLocation(), neighborReq.getApp().getId()));
        if(!neighborReserved.remove(neighborReq))
            throw new IllegalStateException("Couldn't remove neighbor request");

        int neighborR_ =  rd2r_.get(Pair.with(requestID, neighborReq.getLocation()));
        rd2r_.remove(Pair.with(requestID, neighborReq.getLocation()));
        rd2r_.put(Pair.with(requestID, userReq.getLocation()), neighborR_);
        List<Request> userLocReserved = reservedApps.get(Pair.with(userReq.getLocation(), neighborReq.getApp().getId()));
        userLocReserved.remove(userReq);
        neighborReq.setLocation(userReq.getLocation());
        userLocReserved.add(neighborReq);
    }



    public int getDelayOnLinkIfExists(int d1, int d2){
        if (d1==d2)
            return 0;
        if (!dcsInRange[d1][d2])
            return 0;
        return links.get(Pair.with(d1,d2)).getDelay();
    }

    public HashMap<Integer, DC> getDcs() {
        return dcs;
    }

    public HashMap<Pair<Integer, Integer>, Link> getLinks() {
        return links;
    }

    public double getDetRunTime() throws IloException {
        return cplex.getDetTime();
    }

    public double getCplexRunTime() throws IloException {
        return cplex.getCplexTime();
    }

    public int getTime() {
        return time;
    }

    public String getType() {
        return type;
    }

    public List<Request> getOrigRequests() {
        return origRequests;
    }

    public int getNumOfRequests(){
        return requests.size();
    }

    public int getNumOfAllocated() {
        return numOfAllocated;
    }
}



