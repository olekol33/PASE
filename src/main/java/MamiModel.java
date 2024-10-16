import Inputs.*;
import ilog.concert.IloException;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.javatuples.Pair;
import org.javatuples.Quartet;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.util.*;

//Calculate possible locations
//static
//for f1 select node with largest P coverage of possible locations and place f1 -> get f1_coverage
// for each f > f1:
//      select node with largest coverage of possible locations and ensures latency demand  - place f -> gen f_coverage
// check last f returns to use within latency demand
//dynamic
//select node with largest P coverage which is still uncovered -> add to dynamic_instance[i]
//repeat until full coverage achieved

public class MamiModel {
    private List<Request> requests;
    private HashMap<Integer, Function> functions;
    private HashMap<Integer, App> apps;
    private HashMap<Integer, DC> dcs;
    private final HashMap<Pair<Integer,Integer>, Link> links;
    private Set<Integer> existingRequests = new HashSet<>();
    private double[][] procs;
    private int time;
    private HashMap<String, Object> config;
    private HashMap<Integer,HashMap<Integer,HashMap<Integer,vnfPlace>>> requestPlace;
    private int[] staticUnused;
    private String type = "Actual";
    private HashMap<Integer,Boolean> reqPlaced;
    private HashMap<Integer,HashMap<Integer,Integer>> procDelayConst; // <d,f>
    FileHandler fileHandler;
    private Path logdir;

    public HashMap<Integer, HashMap<Integer, HashMap<Integer, vnfPlace>>> getPlacement() {
        return requestPlace;
    }

    public MamiModel(List<Request> requests, HashMap<Integer, Function> functions, HashMap<Integer, App> apps,
                     HashMap<Integer, DC> dcs, HashMap<Pair<Integer, Integer>, Link> links, double[][] probs,
                     HashMap<Integer, List<Double>> probsLW, HashMap<Integer, HashMap<Integer, HashMap<Integer, vnfPlace>>> prevRequestPlace, HashMap<String, Object> config, int time) {
        this.requests = requests;
        this.functions = functions;
        this.apps = apps;
        this.dcs = dcs;
        this.links = links;
        this.procs = probs;
        this.config = config;
        this.time = time;
        this.procDelayConst = new HashMap<>(); // <d,<f,size>> TODO: check enough proc delay before placement
        this.staticUnused = new int[dcs.size()];
        boolean levyWalk = (Boolean)config.get("levyWalk");
        double mamiThreshold = (Double) config.get("mamiThreshold");
        boolean end2endLatency = (Boolean)config.get("end2endLatency");
        this.fileHandler = new FileHandler(config, "mami");
        this.logdir = fileHandler.getLogdir();

        for(int i=0;i<dcs.size();i++)
            staticUnused[i] = 0;

        addProcDelayConst();

        if (prevRequestPlace==null)
            this. requestPlace = new HashMap<>();
        else
            this. requestPlace = prevRequestPlace;

        //remove finished tasks
        Set<Integer> reqIDs = new HashSet<>(); //IDs of current requests
        for (Request r : requests){ //for each request
            reqIDs.add(r.getId());
        }
        List<Integer> removedIDs = new ArrayList<>();
        for(int id : requestPlace.keySet()){
            if(!reqIDs.contains(id)){
                removedIDs.add(id);
                if(requestPlace.get(id)!=null) //if previously existed and placed
                    removePlacedRequest(id);
            }
        }
        for(int i:removedIDs)
            requestPlace.remove(i);

        //Place new requests
        for (Request r : requests){
            r.setWeight(1);
            r.setApp(r.getApp());
            r.setTotalWeight(1);
            if(!requestPlace.containsKey(r.getId())) {
                if(levyWalk){
                    List<Double> userProbsLW = probsLW.get(r.getId());
                    double[] arr = userProbsLW.stream().mapToDouble(Double::doubleValue).toArray();
                    requestPlace.put(r.getId(), processNewRequest(r,arr));
                }
                else
                    requestPlace.put(r.getId(), processNewRequest(r, probs[r.getLocation()]));
            }
        }

        System.out.println(time);
    }

    /** Activates dynamic function on a DC
     *
     *
     * @param rID
     * @param fID
     * @param dcID
     * @return
     */
    private Boolean handleDynamicPlacement(int rID, int fID, int dcID){
        int functionSize = functions.get(fID).getProcSize();
        if(dcs.get(dcID).getCapacity() > functionSize){ //if enough capacity for dynamic
            dcs.get(dcID).updateCapacity(-functionSize, rID,fID );
            dcs.get(dcID).updateDynamicSize(functionSize);
            return true;
        }
        return false;
    }

    /** Removes dynamic allocation
     *
     * @param rID
     * @param fID
     * @param dcID
     */
    private void deactivateDynamicPlacement(int rID, int fID, int dcID){
        int functionSize = functions.get(fID).getProcSize();
        dcs.get(dcID).updateCapacity(functionSize,rID ,fID );
        return;
    }

    /**Get physical placement for this request
     *
     * @param fullPlacement
     * @return
     */
    private HashMap<Integer,vnfPlace> getStaticSFC(HashMap<Integer,HashMap<Integer,vnfPlace>> fullPlacement){
        Iterator it1 = fullPlacement.entrySet().iterator();
        while(it1.hasNext()){
            Map.Entry kv1 = (Map.Entry)it1.next();
            HashMap<Integer,vnfPlace> placement = (HashMap<Integer,vnfPlace>) kv1.getValue();
            if(placement.size()==0)
                continue;
            Iterator it2 = placement.entrySet().iterator();
            Boolean isPhysical = true;
            while(it2.hasNext()){
                Map.Entry kv2 = (Map.Entry)it2.next();
                vnfPlace placed = (vnfPlace) kv2.getValue();
                if(placed.isDynamic()) {
                    isPhysical=false;
                    break; //this is not the physical placement
                }
            }
            if(isPhysical){
                return placement;
            }
            return null; //no physical allocation for this request
        }
        throw new IllegalStateException("No physical placement found");
    }

    /**Counts volume reserved for physical resources which weren't used
     *
     * @param r
     */
    private void updateStaticUnused(Request r, int f, int d){
        if(r!=null) { //update entire chain
            HashMap<Integer, vnfPlace> physicalPlacement = getStaticSFC(requestPlace.get(r.getId()));
            if (physicalPlacement == null)
                return;
            Iterator it = physicalPlacement.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry kv = (Map.Entry) it.next();
                int functionID = (int) kv.getKey();
                vnfPlace v = (vnfPlace) kv.getValue();
                int dc = v.getDc();
                staticUnused[dc] += functions.get(functionID).getProcSize();
            }
        }
        else //update single function
            staticUnused[d] += functions.get(f).getProcSize();
    }

    /** Handles a request. Contains following cases:
     * All placed VNFs are physical and enough link BW -> place
     * Some VNFs are dynamic -> check if enough capacity and place
     * Not enough link or node capacity -> Fail
     */
    public void processRequests(){
        Boolean locationErrorModel = (Boolean)config.get("locationErrorModel");
        double locationErrorRate = (Double) config.get("locationErrorRate");
        RandomGenerator rand = new Well19937c(((int)config.get("seed")));
        float fl;

        reqPlaced = new HashMap<>();
        for (Request r : requests){
            if(requestPlace.get(r.getId())==null) {
                reqPlaced.put(r.getId(),false);
                continue;
            }
            int uLoc = r.getLocation();
            //**Only for location error
            fl = rand.nextFloat();
            if (fl<locationErrorRate && locationErrorModel) {
                uLoc = (uLoc + 1) % dcs.size();
                while(requestPlace.get(r.getId()).get(uLoc)!=null)
                    uLoc = (uLoc + 1) % dcs.size();
            }


            HashMap<Integer,vnfPlace> placement = requestPlace.get(r.getId()).get(uLoc);
            if(placement==null || placement.size()==0){ //no placement for this dc
                reqPlaced.put(r.getId(),false);
                updateStaticUnused(r,-1,-1);
                continue;
            }
            App app = r.getApp();
            Boolean placed = true;
            for(int i=0;i<app.getAppFunctions().size();i++){
                int f = app.getAppFunctions().get(i);
                //function location
                int d =  placement.get(f).getDc();
                Boolean fDynamic = placement.get(f).isDynamic();
                if(fDynamic){
                    if(!handleDynamicPlacement(r.getId(), f, d))
                        placed=false;
                    else
                        updateStaticUnused(null,f,d);
                }
                if(!placed){ //deactivate all up to this f
                    for(int j=0;j<i;j++){
                        int g = app.getAppFunctions().get(j);
                        if(placement.get(g).isDynamic()){
                            d =  placement.get(g).getDc();
                            deactivateDynamicPlacement(r.getId(), g, d);
                        }
                    }
                    break;
                }
            }
            List<Pair<Integer,Integer>> usedLinks = new ArrayList<>();
            for(int i=0;i<app.getPairs().size();i++){ //for each pair
                if(!placed)
                    break;
                int f0 = app.getPairs().get(i).getValue0();
                int f1 = app.getPairs().get(i).getValue1();
                int d0,d1;
                //get locations of functions
                if(f0==0)
                    d0=r.getLocation();
                else {
                    d0 = placement.get(f0).getDc();
                }
                if(f1==0)
                    d1=r.getLocation();
                else {
                    d1 = placement.get(f1).getDc();
                }
                Link link;
                if(d0==d1)
                    continue;
                Pair<Integer,Integer> pair = Pair.with(d0, d1);
                link = links.get(pair);
                int bw = link.getBw();
                if(bw>0) {
                    usedLinks.add(pair);
                    links.get(Pair.with(d0, d1)).updateBW(-1);
                }
                else{ //can't use this link - revert all previous placements
                    for(Pair<Integer,Integer> p:usedLinks) //links
                        links.get(p).updateBW(1);
                    for(int j=0;j<app.getAppFunctions().size();j++){ //dynamic allocations
                        int g = app.getAppFunctions().get(j);
                        if(placement.get(g).isDynamic()){
                            int d =  placement.get(g).getDc();
                            deactivateDynamicPlacement(r.getId(), g, d);
                        }
                    }
                    placed=false;
                    break;
                }
            }
            if(placed)
                reqPlaced.put(r.getId(),true);
            else
                reqPlaced.put(r.getId(),false);
        }
    }


    /** Creates logs of current run
     *
     * @throws FileNotFoundException
     */
    void getSolution() throws FileNotFoundException {

        PrintWriter writer = new PrintWriter(new FileOutputStream(
                new File(this.logdir+"/lp_summary"+"_t"+String.valueOf(time)+"_"+type + ".txt"),
                true /* append = true */));

        File placedAppsFile = new File(this.logdir+"/dump/placedApps.csv");
        File placedFuncFile = new File(this.logdir+"/dump/placedFunctions.csv");
        File dcUtilFile = new File(this.logdir+"/dump/dcUtilization.csv");
        File linkUtilFile = new File(this.logdir+"/dump/linkUtilization.csv");
        PrintWriter capWriter = new PrintWriter(new FileOutputStream(new File(this.logdir+"/capacities.log"), true),true);
        PrintWriter placedAppWriter = new PrintWriter(new FileOutputStream(placedAppsFile, true),true);
        PrintWriter placedFuncWriter = new PrintWriter(new FileOutputStream(placedFuncFile, true),true);
        PrintWriter dcUtilWriter = new PrintWriter(new FileOutputStream(dcUtilFile, true),true);
        PrintWriter linkUtilWriter = new PrintWriter(new FileOutputStream(linkUtilFile, true),true);

        if(placedAppsFile.length()==0)
            placedAppWriter.println("time,type,reqID,dc,appID,size,revenue,predicted,cost,weight,redirected");
        if(placedFuncFile.length()==0)
            placedFuncWriter.println("time,type,reqID,funcID,dc,size");
        if(dcUtilFile.length()==0)
            dcUtilWriter.println("time,type,dc,placed,placedDynamic,staticUnused,capacity,utilization");
        if(linkUtilFile.length()==0)
            linkUtilWriter.println("time,type,src,dst,placed,capacity,utilization");
        writer.println("\n*** " + type + " of interval " + String.valueOf(time) + " ***\n");
        double satSum=0;
        double totalSum=0;
        for (Request req : requests) {
            int rid = req.getId();
            int dSrc = req.getLocation();
            App app = req.getApp();
            String satisfied;


            if(reqPlaced.get(rid)){
                satisfied ="\nV";
                satSum+= 100*req.getWeight();
            }
            else {
                satisfied = "\nX";
                totalSum += 100*req.getWeight();
                continue;
            }
            HashMap<Integer,vnfPlace> placement = requestPlace.get(rid).get(dSrc);



            writer.println(satisfied + " - Request: "+ String.valueOf(rid)+ ", location " +
                    String.valueOf(req.getLocation())+", app "+ req.getAppS()  +", weight "+String.valueOf(req.getWeight()) +
                    ", revenue "+String.valueOf(req.getRevenue()));

            for(int i=0;i<req.getApp().getPairs().size();i++){
                Pair pair = req.getApp().getPairs().get(i);
                int pairCT = req.getApp().getSinglePairCT(i);
                writer.println("{"+ pair.getValue0() + ","+ pair.getValue1() +"} <= " + pairCT + " , ");
            }

            writer.println();
            if(requestPlace.get(rid).get(dSrc)==null || requestPlace.get(rid).get(dSrc).size()==0) //**only for location error mode**
                continue;
            for( int f : req.getApp().getAppFunctions()){
                int d = requestPlace.get(rid).get(dSrc).get(f).getDc();
                String str = "d(" + rid + "," + d + "," + f + ")";
                int c = procDelayConst.get(d).get(f);
                writer.println(c + " X " + str );

                //Functions
                placedFuncWriter.println(time +","+ type +","+ rid +","+  f +","+ d +","+ functions.get(f).getProcSize());
            }

            String predicted = "1";
            for(int i=0; i<req.getApp().getPairs().size();i++){
                int f0 = app.getPairs().get(i).getValue0();
                int f1 = app.getPairs().get(i).getValue1();
                Boolean f0Dynamic=false, f1Dynamic=false;
                int d0,d1;
                //get locations of functions
                if(f0==0)
                    d0=req.getLocation();
                else {
                    d0 = placement.get(f0).getDc();
                    f0Dynamic = placement.get(f0).isDynamic();
                }
                if(f1==0)
                    d1=req.getLocation();
                else {
                    d1 = placement.get(f1).getDc();
                    f1Dynamic = placement.get(f1).isDynamic();
                }
                if(f0Dynamic || f1Dynamic)
                    predicted="0";
                if(d0!=d1){
                    int linkDelay = links.get(Pair.with(d0, d1)).getDelay();
                    Quartet<Integer, Integer, Integer, Pair<Integer, Integer>> quatret = Quartet.with(rid,dSrc, i, Pair.with(d0,d1));
                    writer.println(linkDelay + " X yy" +  Model.quatretYToStr(quatret));
                }
            }



            //Apps
            placedAppWriter.println(time + "," + type + "," + rid + "," + dSrc
                    + "," + req.getAppS() + "," + req.getApp().getTotalSize() + "," +
                    req.getRevenue() + ","+ predicted+ ",0,0,0");//
        }

        totalSum /= 100;
        satSum /= 100;
        totalSum += satSum;
        writer.println("\nSatisfied requests: " + String.valueOf(satSum) + " / " + String.valueOf(totalSum));
        writer.close();


        capWriter.println();
        capWriter.println("---------- " + type +" Demand Analysis Interval "+ String.valueOf(time) + " ----------");
        capWriter.println();

        //Satisfied requests
        Set<Integer> placed = new HashSet<>();
        Iterator it = reqPlaced.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry kv = (Map.Entry) it.next();
            Boolean p = (Boolean)kv.getValue();
            if(p)
                placed.add((Integer) kv.getKey());
        }
        capWriter.println("Placed Requests: " + String.valueOf(placed.size()) + "/" + String.valueOf(reqPlaced.size()));

        List<Integer> placedList = new ArrayList<>();
        placedList.addAll(placed);
        Collections.sort(placedList);
        for(int p=0;p<placedList.size();p++) {
            capWriter.print(String.valueOf(placedList.get(p)) + ", ");
        }
        capWriter.println();

        capWriter.println("DC Utilization:");
        for (int d=0; d< dcs.size();d++) {
            BigDecimal placedDynamic = new BigDecimal(dcs.get(d).getDynamicSize());
            BigDecimal diff = new BigDecimal(dcs.get(d).getOriginalCapacity()- dcs.get(d).getCapacity());
            BigDecimal c = new BigDecimal(dcs.get(d).getOriginalCapacity());
//            BigDecimal del = new BigDecimal(dcs.get(d).getCapacity());
            BigDecimal utilization = diff.divide(c,2, RoundingMode.HALF_UP);
            utilization = utilization.multiply(new BigDecimal(100));
            capWriter.println("DC " + String.valueOf(d) + ": " + String.valueOf(diff) + "/"+String.valueOf(c) + " (" +
                    String.valueOf(utilization.intValue()) + "%)");
            dcUtilWriter.println(String.valueOf(time) +","+ type +","+String.valueOf(d)+","+String.valueOf(diff)+","+String.valueOf(placedDynamic.intValue())+
                    ","+ String.valueOf(staticUnused[d]) +","+String.valueOf(c)+","+String.valueOf(utilization.intValue()));
        }

        //Link Capacity
        capWriter.println();
        capWriter.println("Link Utilization:");
        int[][] usedLinks = new int[dcs.size()][dcs.size()];
        int[][] bw = new int[dcs.size()][dcs.size()];
        for(int[] row : usedLinks){
            Arrays.fill(row,0);
        }

        it = links.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry kv = (Map.Entry) it.next();
            Pair<Integer,Integer> pair = (Pair) kv.getKey();
            Link link = (Link) kv.getValue();
            bw[pair.getValue0()][pair.getValue1()] = link.getBw();
        }

        for(int i=0; i< dcs.size();i++){
            for (int j=0; j< dcs.size(); j++){
                if(bw[i][j] > 0){
                    BigDecimal diff = new BigDecimal(links.get(Pair.with(i,j)).getOriginalBW() - bw[i][j]);
                    BigDecimal c = new BigDecimal(links.get(Pair.with(i,j)).getOriginalBW());
                    BigDecimal y = new BigDecimal(usedLinks[i][j]);
                    y = y.add(diff);
//                    if(y.intValue()<0)
//                        throw new IllegalStateException("BW<0: " + String.valueOf(bw[i][j]));
                    BigDecimal utilization = y.divide(c,2, RoundingMode.HALF_UP);
                    utilization = utilization.multiply(new BigDecimal(100));
                    capWriter.println("Link {" + String.valueOf(i) +","+String.valueOf(j)+
                            "}: " + String.valueOf(y) + "/"+String.valueOf(c) + " (" +
                            String.valueOf(utilization.intValue()) + "%)");
                    linkUtilWriter.println(String.valueOf(time) +","+ type +","+String.valueOf(i) +","+String.valueOf(j) +
                            ","+String.valueOf(y) +","+String.valueOf(c)+","+String.valueOf(utilization.intValue()));
                    //Updated non-utilized capacity
//                    links.get(Pair.with(i,j)).setBw(links.get(Pair.with(i,j)).getBw() - usedLinks[i][j]);
                    links.get(Pair.with(i,j)).updateBW(-usedLinks[i][j]);
                    if(links.get(Pair.with(i,j)).getBw()<0)
                        throw new IllegalStateException("BW<0");
                }
            }
        }




    }


    /**
     * Count processing cost of function in each dc
     * @throws IloException if something wrong with Cplex
     */
    private void addProcDelayConst()
    {
        for (int d=0; d< dcs.size(); d++)
        {
            HashMap<Integer,Integer> dc = new HashMap<>();
            for (int f=1; f<= functions.size(); f++){
                int c = dcs.get(d).getProcPower()* functions.get(f).getProcSize();
                dc.put(f,c);
            }
            procDelayConst.put(d,dc);
        }
    }

    /** Remove placed VNFs following a request which is no longer valid
     *
     * @param reqID
     */
    void removePlacedRequest(int reqID){
        HashMap<Integer,HashMap<Integer,vnfPlace>> placement = requestPlace.get(reqID);

        for(int d:placement.keySet()){
            Boolean fullyPhysical = true;
            HashMap<Integer,vnfPlace> dcPlace = placement.get(d);
            for(int reqDcPlace:dcPlace.keySet()){
                if(dcPlace.get(reqDcPlace).isDynamic()){ //find the dc for which physical chain was placed
                    fullyPhysical=false;
                    break;
                }
            }
            if(fullyPhysical){ //if this is the desired dc
                for(int vnfID:dcPlace.keySet()){ //now iterate over placement and remove
                    vnfPlace vnf = dcPlace.get(vnfID);
                    int functionSize = functions.get(vnfID).getProcSize();
                    dcs.get(vnf.getDc()).updateCapacity(functionSize,reqID ,vnfID );
                }
                return;
            }
        }
        throw new IllegalStateException("Placement not removed");
    }


    /**
     * Returns locations in which user might be present according to probability map
     * */
    private Set<Integer> getRequestCoverage(double[] probs){
        double th = 0;
        Set<Integer> dcs= new HashSet<>();
        for (int i=0;i<probs.length;i++){
            if(probs[i] > th)
                dcs.add(i);
        }
        return dcs;
    }

//    private Set<Integer> legallyCoveredNodes


    /**
     * For first VNF launched from current user location
     * check which placement covers most possible user locations
     * Returns list of nodes covered by selected placement. First item in returned list is placed node.
     */
    private List<Integer> getLargestCoveringFirstDC(double[] probs, int functionID, int srcDC, int pairConstraint,
                                                    Boolean isDynamic, Set<Integer> excludedNodes){
        Set<Integer> coverageArea = getRequestCoverage(probs);
        int functionSize = functions.get(functionID).getProcSize();
        double maxCoverageProb = 0;
        List<Integer> maxCoveredNodes = null;
        List<Integer> legalNodes = new ArrayList<>();
        if(dcs.get(srcDC).getCapacity()>=functionSize || isDynamic) {
            if(!excludedNodes.contains(srcDC) && probs[srcDC]>0) //probs[srcDC]>0 only for LW
                legalNodes.add(srcDC);
        }
        for(int d : coverageArea){ //find nodes that can contain first VNF
            if((dcs.get(d).getCapacity()<functionSize && !isDynamic) || excludedNodes.contains(d)) //check node capacity
                continue;
            if (srcDC!=d) { //make sure has link to src node
                Link link = links.get(Pair.with(srcDC,d));
                int delayFromUser = link.getDelay();
                int processingDelay = procDelayConst.get(d).get(functionID);
                if (delayFromUser==0 || delayFromUser+processingDelay>pairConstraint || link.getBw()==0) //no such link || delay > constraint || no capacity
                    continue;
                else
                    legalNodes.add(d);
            }
        }
        //for each possible first VNF location
        //check which one covers most user locations
        for(int ddest : legalNodes){
            double coverageProb = 0;
            List<Integer> coveredNodes = new ArrayList<>();
            //add self
            coverageProb += probs[ddest];
            coveredNodes.add(ddest);
            for (int dsrc : coverageArea){
                if(dsrc==ddest)
                    continue;
                else{
                    int delayFromUser = links.get(Pair.with(dsrc, ddest)).getDelay();
                    if (delayFromUser==0 || delayFromUser>pairConstraint) //no such link || delay > constraint
                        continue;
                    coverageProb += probs[dsrc];
                    coveredNodes.add(dsrc);
                }
            }
            if (maxCoverageProb < coverageProb) { //keep list
                if(dcs.get(coveredNodes.get(0)).getCapacity()<functionSize && !isDynamic)
                    throw new IllegalStateException("Unexpected capacity value");
                maxCoverageProb = coverageProb;
                maxCoveredNodes = new ArrayList<>(coveredNodes);


            }
        }
        if(maxCoveredNodes!=null && excludedNodes.contains(maxCoveredNodes.get(0)))
            throw new IllegalStateException("Cannot be placed on excluded node");
        return maxCoveredNodes;
    }

    /** For a DC d get list of nodes in which subsequent VNF can be placed
     *
     * @param d
     * @param findNeighbors
     * @return List of legal DCs
     */
    private List<Integer> getLegalDCs(int d, Boolean findNeighbors){
        List<Integer> legalDCs = new ArrayList<>();
        for(int i=0;i< dcs.size();i++){
            if(i==d && !findNeighbors) { //self
                if (dcs.get(i).getCapacity() > 0) {
                    legalDCs.add(i);
                    continue;
                }
            }
            if(links.containsKey(Pair.with(d,i))){
                if(links.get(Pair.with(d,i)).getBw()>0){
                    if(findNeighbors)
                        legalDCs.add(i);
                    else if(dcs.get(i).getCapacity()>0)
                        legalDCs.add(i);
                }
            }
        }
        return legalDCs;
    }

    //Places SFC in list "vnfs" according to constraints and checks path back to srcDC
    private List<Integer> placeSFC(int srcDC, int firstVnfDC, List<Integer> vnfs, List<Integer> constrains,
                                   Boolean isDynamic, Request req){
        List<Integer> placedDCs = new ArrayList<>();
        int prevDC = firstVnfDC;
        for(int i=0; i<vnfs.size(); i++){
            int vnf = vnfs.get(i);
            int vnfCT = constrains.get(i);
            List<Integer> feasibleDCs = getLegalDCs(prevDC, false); //Initial filtering of optional DCs for placement
            boolean isLastPair = i == (vnfs.size() - 1);
            while(!feasibleDCs.isEmpty()) {
                //try optional DCs
                if (feasibleDCs.contains(prevDC)) { //first try same DC as prev - check only link to src for last pair
                    int targetDC = prevDC;
                    int linkDelay = 0;
                    if(prevDC!=srcDC)
                        linkDelay = links.get(Pair.with(targetDC,srcDC)).getDelay();
                    int processingDelay = procDelayConst.get(targetDC).get(vnf);
                    if(isDynamic){
                        if(isLastPair) {
                            if(linkDelay + processingDelay > vnfCT) {
                                if(placedDCs.size() > 1 && vnfs.size() == 1)
                                    throw new IllegalStateException("Haven't released reserved resources");
                                placedDCs.addFirst(-1); //started from illegal placement - retry with new first DC
                                return placedDCs;
                            }
                        }
                        else{
                            if(processingDelay > vnfCT){
                                feasibleDCs.remove(Integer.valueOf(targetDC)); //retry
                                continue;
                            }
                        }
                        placedDCs.add(targetDC);
                        break;
                    }
                    if(dcs.get(targetDC).getCapacity() >= functions.get(vnf).getProcSize()) { //if has enough capacity
                        if(isLastPair){ //if last pair and not in user location
                            if((linkDelay + processingDelay > vnfCT)) { //if enough delay
                                if(placedDCs.size() > 1 && vnfs.size() == 1)
                                    throw new IllegalStateException("Haven't released reserved resources");
                                placedDCs.addFirst(-1); //started from illegal placement - retry with new first DC
                                return placedDCs;
                            }
                        }
                        else{
                            if((processingDelay>vnfCT)) { //if enough delay
                                feasibleDCs.remove(Integer.valueOf(targetDC)); //retry
                                continue;
                            }
                        }
                        if(!isDynamic) {
                            dcs.get(targetDC).updateCapacity(-functions.get(vnf).getProcSize(),req.getId() ,vnf );
                        }
                        placedDCs.add(targetDC);
                        break;
                    }
                    else
                        feasibleDCs.remove(Integer.valueOf(targetDC)); //retry
                }
                if (feasibleDCs.contains(srcDC) && prevDC!=srcDC) { //place in DC or user location (src)
                    int targetDC = srcDC;
                    int linkDelay = links.get(Pair.with(prevDC,srcDC)).getDelay();
                    int processingDelay = procDelayConst.get(targetDC).get(vnf);
                    if(isDynamic){
                        if(isLastPair) {
                            if ((linkDelay + processingDelay > vnfCT)) {
//                                throw new IllegalStateException("No path to src");
                                feasibleDCs.remove(Integer.valueOf(targetDC)); //retry (no path to src)
                                continue;
                            }
                        }
                        else {
                            if (processingDelay > vnfCT) {
                                feasibleDCs.remove(Integer.valueOf(targetDC)); //retry
                                continue;
                            }
                        }
                        placedDCs.add(targetDC);
                        break;
                    }
                    if(dcs.get(targetDC).getCapacity()>=functions.get(vnf).getProcSize()) { //if has enough capacity
                        if((linkDelay+processingDelay<=vnfCT)) { //if  delay
                            if(!isDynamic)
                                dcs.get(targetDC).updateCapacity(-functions.get(vnf).getProcSize(),req.getId() ,vnf );
                            prevDC = targetDC;
                            placedDCs.add(targetDC);
                            break;
                        }
                        else {
                            feasibleDCs.remove(Integer.valueOf(targetDC));
                            continue;
                        }
                    }
                    feasibleDCs.remove(Integer.valueOf(targetDC)); //retry
                }
                //not src nor prev
                if(feasibleDCs.isEmpty()) //none remaining
                    return null;
                int d = feasibleDCs.getFirst();
                int targetDC = d;
                //check if has link to src
                List<Integer> legalNeighbors = getLegalDCs(d, true);
                if(!legalNeighbors.contains(srcDC)){
                    feasibleDCs.remove(Integer.valueOf(d)); //retry
                }
                else{
                    int linkDelay = links.get(Pair.with(prevDC,d)).getDelay();
                    int processingDelay = procDelayConst.get(targetDC).get(vnf);
                    if(isDynamic){
                        if(isLastPair) {
                            if ((linkDelay + processingDelay > vnfCT)) {
                                feasibleDCs.remove(Integer.valueOf(targetDC)); //retry (no path to src)
                                continue;
                            }
                        }
                        else {
                            if (processingDelay > vnfCT) {
                                feasibleDCs.remove(Integer.valueOf(targetDC)); //retry
                                continue;
                            }
                        }
                        placedDCs.add(targetDC);
                        break;
                    }
                    if(dcs.get(targetDC).getCapacity()>=functions.get(vnf).getProcSize()) { //if has enough capacity
                        if ( (linkDelay+processingDelay<=vnfCT)) { //if enough delay
                            if(isLastPair) {
                                Link linkToSrc = links.get(Pair.with(d, srcDC));
                                if((linkToSrc.getDelay() + linkDelay + processingDelay > vnfCT)){ //enough delay and BW for last
                                    feasibleDCs.remove(Integer.valueOf(d));
                                }
                            }
                            if(feasibleDCs.contains(d)) { //check if not removed in last pair
                                if (!isDynamic) {
                                    dcs.get(targetDC).updateCapacity(-functions.get(vnf).getProcSize(),req.getId() ,vnf );
                                }
                                prevDC = targetDC;
                                placedDCs.add(targetDC);
                                break;
                            }
                        }
                    }
                    feasibleDCs.remove(Integer.valueOf(targetDC)); //retry
                }
            }
            if(placedDCs.size() != i+1) //total placed VNF until now
                break;
        }
        if(placedDCs.size() < vnfs.size()){
            if (!isDynamic) {
                for (int i = 0; i < placedDCs.size(); i++) { //remove placed vnf
                    int targetDC = placedDCs.get(i);
                    int functionSize = functions.get(vnfs.get(i)).getProcSize();
                    dcs.get(targetDC).updateCapacity(functionSize,req.getId() ,vnfs.get(i) );
                }
            }
            return null;
        }
        return placedDCs;
    }

    /** Check if actual placement follows constraints
     *
     * @param r
     * @param srcDC
     * @param placedDCs
     * @return
     */
    private Boolean isPlacementLegal(Request r, int srcDC, List<Integer> placedDCs){
        HashMap<Integer,Pair<Integer,Integer>> logicalPairs = r.getApp().getPairs();
        HashMap<Integer,Pair<Integer,Integer>> physicalPairs = new HashMap<>();
        HashMap<Integer,Integer> pairsCTs = r.getApp().getPairCTs();

        //create physical pairs
        physicalPairs.put(0,Pair.with(srcDC,placedDCs.getFirst()));
        physicalPairs.put(placedDCs.size(),Pair.with(placedDCs.getLast(),srcDC));
        for(int i=1;i<=placedDCs.size()-1;i++){
            physicalPairs.put(i,Pair.with(physicalPairs.get(i-1).getValue1(),placedDCs.get(i)));
        }

        if(logicalPairs.size()==3){ //single logical pair
            int totalDelay=0;
            //total delay of pairs
            for(Pair<Integer,Integer> pair:physicalPairs.values()){
                if(!Objects.equals(pair.getValue0(), pair.getValue1()))
                    totalDelay += links.get(pair).getDelay();
            }
            //total processing delay
            for(int i=0;i< placedDCs.size();i++){
                totalDelay += dcs.get(placedDCs.get(i)).getProcPower()*functions.get(r.getApp().getAppFunctions().get(i)).getProcSize();
            }
            return totalDelay <= pairsCTs.get(1);
        }

        //pair constraint is for processing delay+subsequent link
        for(int i=1;i<logicalPairs.size()-1;i++){
            int totalDelay=0;
            if(i==1){ //first logical pair
                for(int j=0;j<=1;j++) { //pair from src and first logical pair (2 in total)
                    Pair<Integer, Integer> pair = physicalPairs.get(j);
                    if (!Objects.equals(pair.getValue0(), pair.getValue1()))
                        totalDelay += links.get(pair).getDelay();
                }
                totalDelay += dcs.get(placedDCs.getFirst()).getProcPower()*functions.get(r.getApp().getAppFunctions().getFirst()).getProcSize();
            }
            else if(i==logicalPairs.size()-2){ //last logical pair
                for(int j=physicalPairs.size()-1;j<physicalPairs.size();j++) { //last logical pair and pair to src
                    Pair<Integer, Integer> pair = physicalPairs.get(physicalPairs.size()-1);
                    if (!Objects.equals(pair.getValue0(), pair.getValue1()))
                        totalDelay += links.get(pair).getDelay();
                }
                totalDelay += dcs.get(placedDCs.getLast()).getProcPower()*
                        functions.get(r.getApp().getAppFunctions().get(placedDCs.size()-1)).getProcSize();
            }
            else{
                //link
                Pair<Integer, Integer> physicalPair = physicalPairs.get(i);
                Pair<Integer, Integer> logicalPair = logicalPairs.get(i);
                if (!Objects.equals(physicalPair.getValue0(), physicalPair.getValue1()))
                    totalDelay += links.get(physicalPair).getDelay();
                //processing in pair of DCs (count only first)
                totalDelay += dcs.get(physicalPair.getValue0()).getProcPower()*functions.get(logicalPair.getValue0()).getProcSize(); //first DC
//                totalDelay += dcs.get(physicalPair.getValue1()).getProcPower()*functions.get(logicalPair.getValue1()).getProcSize(); //second DC
            }

            if (totalDelay > pairsCTs.get(i))
                return false;
        }
        return true;
    }


    //For each possible user location calculate placement for all VNFs
    //Begin with starting DC and calculate physical instance first
    private HashMap<Integer,HashMap<Integer,vnfPlace>> processNewRequest(Request req, double[] probs){
        double[] tempProbs = new double[probs.length];
        int probsTh=0;
        int firstVnfDC=-1;
        List<Integer> placedDCs = null;
        List<Integer> remainingVNFs = null;
        List<Integer> vnfConstraints = null;
        List<Integer> optionalDCs = new ArrayList<>();
        List<Integer> feasibleDCs = null;
        Set<Integer> excludedNodes = new HashSet<>();
        HashMap<Integer,HashMap<Integer,vnfPlace>> requestPlace = new HashMap<>(); //dc, <vnfID, vnfPlace>
        Set<Integer> userCoverage = getRequestCoverage(probs);
        vnfPlace vnfPlace;
        int srcDC = req.getLocation();
        int pairCT;


        for(int i=0; i<probs.length; i++) {
            if(probs[i] > probsTh)
                optionalDCs.add(i);
        }
        for (int d : userCoverage)
            requestPlace.put(d,new HashMap<Integer,vnfPlace>());

        // Zero pair
        int firstVNF = req.getApp().getPairs().get(0).getValue1();
        int functionSize = functions.get(firstVNF).getProcSize();
        boolean completedPlacement = false;
        while (!completedPlacement) {
            pairCT = req.getApp().getPairCTs().get(1);
            feasibleDCs = getLargestCoveringFirstDC(probs, firstVNF, srcDC, pairCT, false, excludedNodes);
            if (feasibleDCs == null) //can't place first VNF
                return null;
            firstVnfDC = feasibleDCs.get(0);
            pairCT -= procDelayConst.get(firstVnfDC).get(firstVNF); //processing delay of first VNF
            dcs.get(firstVnfDC).updateCapacity(-functionSize,req.getId() ,firstVNF );
            if (srcDC != firstVnfDC) { //update link BW
                Link link = links.get(Pair.with(srcDC, firstVnfDC));
                pairCT -= link.getDelay(); //deduct delay from user to first VNF

            }
            remainingVNFs = new ArrayList<>();
            vnfConstraints = new ArrayList<>();

            //Remaining pairs
            for (int i = 1; i < req.getApp().getPairs().size() - 1; i++) {
                remainingVNFs.add(req.getApp().getPairs().get(i).getValue1());
                if (i == 1) //pair 1
                    vnfConstraints.add(pairCT); //remaining CT
                else
                    vnfConstraints.add(req.getApp().getPairCTs().get(i));
            }

            //place physical
            placedDCs = placeSFC(srcDC, firstVnfDC, remainingVNFs, vnfConstraints, false, req);
            if(placedDCs==null) { //failed to place - revert
                dcs.get(firstVnfDC).updateCapacity(functionSize,req.getId() ,firstVNF );
                excludedNodes.add(firstVnfDC);
                continue;
//                return null;
            }
            else if(placedDCs.getFirst()==-1) {//illegal state for current placement - retry
                excludedNodes.add(firstVnfDC);
                dcs.get(firstVnfDC).updateCapacity(functionSize,req.getId() ,firstVNF );
            }
            else {
                completedPlacement = true;
                placedDCs.addFirst(firstVnfDC);
                updateFeasibleDCs(req, placedDCs, srcDC, feasibleDCs);
                //place first VNF in covered nodes
                vnfPlace = new vnfPlace(feasibleDCs.getFirst(), false);
                for (int d : feasibleDCs) { //for each feasible DC add to dataset
                    HashMap<Integer, vnfPlace> placedSFC = requestPlace.get(d);
                    placedSFC.put(firstVNF, vnfPlace);
                    requestPlace.put(d, placedSFC);
                }
            }
        }
        if(placedDCs==null)
            return null;
        remainingVNFs.addFirst(req.getApp().getPairs().get(0).getValue1()); //add first VNF (after user func)
        placedDCs.addFirst(firstVnfDC);
        //For optional locations that used first VNF - can use remaining VNFs (assume symmetry on link delay)
        for (int i = 0; i < remainingVNFs.size(); i++) {
            int dc = placedDCs.get(i);
            int vnf = remainingVNFs.get(i);
            vnfPlace = new vnfPlace(dc, false);
            for (int d : feasibleDCs) { //for each node covering this VNF add to dataset
                HashMap<Integer, vnfPlace> placedSFC = requestPlace.get(d);
                placedSFC.put(vnf, vnfPlace);
                requestPlace.put(d, placedSFC);
            }
        }
        remainingVNFs.removeFirst(); //once placed, remove first item (used below)

        //Dynamic (if not all fully placed)
        optionalDCs.removeAll(feasibleDCs); //remove fully placed
        while(!optionalDCs.isEmpty()){
            int maxProbDC=-1;
            int probMax = 0;
            completedPlacement=false;
            for(int d:optionalDCs) {
                tempProbs[d] = probs[d];
                if(tempProbs[d]>probMax)
                    maxProbDC=d;
            }
            srcDC = maxProbDC;
            pairCT = req.getApp().getPairCTs().get(1);
            excludedNodes = new HashSet<>();
            while(!completedPlacement) {
                feasibleDCs = getLargestCoveringFirstDC(tempProbs, firstVNF, srcDC, pairCT, true, excludedNodes);
                if(feasibleDCs==null) {
                    optionalDCs.remove(Integer.valueOf(srcDC)); //no placement options remain
                    break;
                }
                firstVnfDC = feasibleDCs.getFirst();
                if (srcDC != firstVnfDC) { //update link BW
                    Link link = links.get(Pair.with(srcDC, firstVnfDC));
                    pairCT -= link.getDelay(); //deduct delay from user to first VNF
                }
                vnfConstraints.set(0, pairCT); //update constraint of first pair
                placedDCs = placeSFC(srcDC, firstVnfDC, remainingVNFs, vnfConstraints, true, req);
                if (placedDCs == null) { //won't finish dynamic placement
                    optionalDCs.remove(Integer.valueOf(srcDC)); //no placement options remain
                    break;
                }
                else if(placedDCs.getFirst()==-1) {//illegal state for current placement - retry
                    excludedNodes.add(firstVnfDC);
                    continue;
                }
                else {
                    placedDCs.addFirst(firstVnfDC);
                    ListIterator<Integer> iter = feasibleDCs.listIterator();
                    boolean retry=false;
                    while(iter.hasNext()){
                        int d = iter.next();
                        if(!isPlacementLegal(req, d, placedDCs)){
                            if(d==srcDC) { //failed to include srcDC in coverage
                                excludedNodes.add(firstVnfDC);
                                retry=true;
                                break;
                            }
                            iter.remove();
                        }
                    }
                    if(retry)
                        continue;
                    else
                        completedPlacement = true;
                }
                remainingVNFs.addFirst(req.getApp().getPairs().get(0).getValue1());
                placedDCs.addFirst(firstVnfDC);
                for (Integer integer : feasibleDCs) tempProbs[integer] = 0;
                optionalDCs.removeAll(feasibleDCs);
                //place
                for (int i = 0; i < remainingVNFs.size(); i++) {
                    int dc = placedDCs.get(i);
                    int vnf = remainingVNFs.get(i);
                    vnfPlace = new vnfPlace(dc, true);
                    for (int d : feasibleDCs) { //for each node covering this VNF add to dataset
                        HashMap<Integer, vnfPlace> placedSFC = requestPlace.get(d);
                        placedSFC.put(vnf, vnfPlace);
                        requestPlace.put(d, placedSFC);
                    }
                }
                remainingVNFs.removeFirst(); //once placed, remove first item
            }
        }

        return requestPlace;
    }

    private void updateFeasibleDCs(Request req, List<Integer> placedDCs, int srcDC, List<Integer> coveredSet){
        ListIterator<Integer> iter = coveredSet.listIterator();
        while(iter.hasNext()){
            int d = iter.next();
            if(!isPlacementLegal(req, d, placedDCs)){
                if(d!=srcDC)
                    iter.remove();
            }
        }
    }
}
