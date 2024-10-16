package Inputs;

import ilog.concert.IloException;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.javatuples.Pair;


import java.util.*;


public class App {
//    types.Pair<String,String> pair = new types.Pair<>();
    private int id;
    private HashMap<Integer,Pair<Integer,Integer>> pairs;
    private HashMap<Integer,Integer> pairCTs;
    private List<Integer> appFunctions;
    int numOfPairs;
    int totalSize;



    public int getId() {
        return id;
    }

    /**
     * Gets an array of functions pairs that belong to app
     * @return Array of pairs of function IDs
     * @throws IloException if something wrong with Cplex
     */
    public HashMap<Integer,Pair<Integer,Integer>> getPairs() {
        return pairs;
    }

    public HashMap<Integer,Integer> getPairCTs() {
        return pairCTs;
    }
    public Integer getSinglePairCT(int pairID) {
        return pairCTs.get(pairID);
    }

    private static final int ID=0;
    private static final int PAIR_ID=1;
    private static final int PAIR=2;
    private static final int PAIR_CT=3;

    public List<Integer> getAppFunctions() {
        return appFunctions;
    }

    public App(HashMap<Integer,App> applications, HashMap<Integer,Function> functions, String[] app){
        id = Integer.valueOf(app[ID]);
        int pairID = Integer.valueOf(app[PAIR_ID]);
        String[] pair = app[PAIR].split("-");
        int pairCT = Integer.valueOf(app[PAIR_CT]);

        //If pairs of this app already logged
        addToExistingApp(applications,functions,pairID,pairCT,pair);
        checkAppValidity(functions.keySet(),pair);
    }

    public int getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(int totalSize) {
        this.totalSize = totalSize;
    }


    private void addToExistingApp(HashMap<Integer,App> applications, HashMap<Integer,Function> functions, int pairID,
                                  int pairCT, String[] pair){
        if(applications.containsKey(id)){
            pairs = applications.get(id).getPairs();
            pairCTs = applications.get(id).getPairCTs();
            //if this pair isn't logged
            if(!pairs.containsKey(pairID)) {
                Pair<Integer,Integer> newPair = new Pair<>(Integer.valueOf(pair[0]),Integer.valueOf(pair[1]));
                pairs.put(pairID,newPair);
                appFunctions = applications.get(id).getAppFunctions();
                //assuming only second function is new
                if(!appFunctions.contains(newPair.getValue1())) {
                    appFunctions.add(newPair.getValue1());
                    totalSize = applications.get(id).getTotalSize()+ functions.get(newPair.getValue1()).procSize;
                }
                else
                    throw new IllegalStateException("Expected function to be new");
                pairCTs.put(pairID,pairCT);
            }
            else{
                throw new IllegalStateException("Already have this pair in application " + id);
            }
        }
        else{ //new app
            totalSize = 0;
            pairs = new HashMap<>();
            Pair<Integer,Integer> newPair = new Pair<>(Integer.valueOf(pair[0]),Integer.valueOf(pair[1]));
            pairs.put(pairID,newPair);
            appFunctions = new ArrayList<>();
            appFunctions.add(newPair.getValue0());
            appFunctions.add(newPair.getValue1());
            totalSize += functions.get(newPair.getValue0()).procSize;
            totalSize += functions.get(newPair.getValue1()).procSize;

            pairCTs = new HashMap<>();
            pairCTs.put(pairID,pairCT);

            if(pairs.size()!=pairID)
                throw new IllegalStateException("Sizes don't match in app, perhaps incorrect order of pairs. Inputs.App ID = " + id);
        }
    }

    /**
     * Checks if all functions in application are from functions set
     * @throws IllegalArgumentException if function incorrect
     */
    private void checkAppValidity(Set<Integer> functions, String[] pair){
        if(!functions.contains(Integer.valueOf(pair[0])) || !functions.contains(Integer.valueOf(pair[0]))) {
            throw new IllegalArgumentException("illegal pair " + pair[0] + ", " + pair[1]);
        }
/*        else{
        }*/
    }

    public static int sampleZipf(RandomGenerator rand, int n, double zipfExponent){
        return new ZipfDistribution(rand, n, zipfExponent).sample();
    }

    public static int sampleZipf(RandomGenerator rand, int n){
        double zipfExponent = 1.07;
        return new ZipfDistribution(rand, n, zipfExponent).sample();
    }

    /**
     * Synthetic generation of applications based on config and input functions
     * @param config input config
     * @param functions list of functions
     */
    public static HashMap<Integer, App> genApps(HashMap<String,Object> config, HashMap<Integer,Function> functions){
        boolean normalDistApps = (boolean)config.get("normalDistApps");
        boolean zipfDistApps = (boolean)config.get("zipfDistApps");
        if (normalDistApps && zipfDistApps)
            throw new IllegalStateException("normalDistApps && zipfDistApps");
        HashMap<Integer,App> apps;
        if (normalDistApps)
            apps = genNormDistApps(config, functions);
        else if (zipfDistApps)
            apps = genZipfApps(config, functions);
        else
            apps = getDeterministicApps(config, functions);
/*        if ((Boolean) config.get("nonSplitMode")) {
            for (int i = 0; i < 2; i++)
                createVirtualFunctions(functions);
        }*/
        //add zero functions to mark location of user
        for (Map.Entry<Integer, App> kv : apps.entrySet()) {
            App app = kv.getValue();
            app.addZeroFunctions(functions, config);
        }
        return apps;
    }

    private static HashMap<Integer,App> genZipfApps(HashMap<String,Object> config, HashMap<Integer,Function> functions){
        double meanPairConstraint = (double)config.get("meanPairConstraint");
        double stdPairConstrains = (double)config.get("stdPairConstrains");
        int seed = (int)config.get("seed");
        int numberOfApps = (int)config.get("numberOfApps");
        double meanAppPairs = (double)config.get("meanAppPairs");
        double appPairsStd = (double)config.get("appPairsStd");
        HashMap<Integer,App> apps = new HashMap<>();
        RandomGenerator rand = new Well19937c((seed));
        List<Integer> funcList = new ArrayList<>(functions.keySet());
        Collections.shuffle(funcList,new Random(seed));
        for (int a = 1; a <= numberOfApps; a++) {
            int numOfPairs;
            NormalDistribution normalPairs = null;
            numOfPairs = sampleZipf(rand, (int) meanAppPairs, 1) + 1;
            NormalDistribution normalCTs = new NormalDistribution(rand, meanPairConstraint, stdPairConstrains);
            while (numOfPairs <= 0 || numOfPairs > funcList.size())
                numOfPairs = (int) normalPairs.sample();
            Set<Integer> usedFuncs = new HashSet<>();
            int f1 = 0;
            f1 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
            usedFuncs.add(f1);
            for (int p = 1; p <= numOfPairs; p++) {
                int f2 = 0;
                f2 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
                while (usedFuncs.contains(f2))
                    f2 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
                usedFuncs.add(f2);

                int CT = (int) normalCTs.sample();
                while (CT <= 0)
                    CT = (int) normalCTs.sample();

                String[] app = new String[4]; //appID,pairID,pair,pairCT
                app[0] = String.valueOf(a);
                app[1] = String.valueOf(p);
                app[2] = String.valueOf(f1) + "-" + String.valueOf(f2);
                app[3] = String.valueOf(CT);

                App app1 = new App(apps, functions, app);
                apps.put(app1.getId(), app1);
                f1 = f2; //for next pair
            }
        }
        return apps;
    }
    private static HashMap<Integer,App> genNormDistApps(HashMap<String,Object> config, HashMap<Integer,Function> functions){
        double meanPairConstraint = (double)config.get("meanPairConstraint");
        int seed = (int)config.get("seed");
        int numberOfApps = (int)config.get("numberOfApps");
        double meanAppPairs = (double)config.get("meanAppPairs");
        double appPairsStd = (double)config.get("appPairsStd");
        HashMap<Integer,App> apps = new HashMap<>();
        RandomGenerator rand = new Well19937c((seed));
        List<Integer> funcList = new ArrayList<>(functions.keySet());
        Collections.shuffle(funcList,new Random(seed));
        for (int a = 1; a <= numberOfApps; a++) {
            int numOfPairs;
            NormalDistribution normalPairs = null;
            if (appPairsStd > 0) {
                normalPairs = new NormalDistribution(rand, meanAppPairs, appPairsStd);
                numOfPairs = (int) normalPairs.sample();
            } else
                numOfPairs = (int) meanAppPairs;
            NormalDistribution normalCTs = new NormalDistribution(rand, meanPairConstraint, 1);
            while (numOfPairs <= 0 || numOfPairs > funcList.size())
                numOfPairs = (int) normalPairs.sample();
            Set<Integer> usedFuncs = new HashSet<>();
            int f1 = 0;
            f1 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
            usedFuncs.add(f1);
            for (int p = 1; p <= numOfPairs; p++) {
                int f2 = 0;
                f2 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
                while (usedFuncs.contains(f2))
                    f2 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
                usedFuncs.add(f2);

                int CT = (int) normalCTs.sample();
                while (CT <= 0)
                    CT = (int) normalCTs.sample();

                String[] app = new String[4]; //appID,pairID,pair,pairCT
                app[0] = String.valueOf(a);
                app[1] = String.valueOf(p);
                app[2] = String.valueOf(f1) + "-" + String.valueOf(f2);
                app[3] = String.valueOf(CT);

                App app1 = new App(apps, functions, app);
                apps.put(app1.getId(), app1);
                f1 = f2; //for next pair
            }
        }
        return apps;
    }

    private static HashMap<Integer,App> getDeterministicApps(HashMap<String,Object> config, HashMap<Integer,Function> functions){
        double meanPairConstraint;
        boolean normalDistLatencyConstAppSize = (boolean)config.get("normalDistLatencyConstAppSize");
/*        if ((Boolean) config.get("nonSplitMode"))
            meanPairConstraint = (int) config.get("meanFunctionSize")*2 + (int) config.get("delay") - 1;
        else*/
            meanPairConstraint = (double)config.get("meanPairConstraint");
        int seed = (int)config.get("seed");
        int numberOfApps = (int)config.get("numberOfApps");
        RandomGenerator rand = new Well19937c((seed));
        List<Integer> funcList = new ArrayList<>(functions.keySet());
        Collections.shuffle(funcList,new Random(seed));
        HashMap<Integer,App> apps = new HashMap<>();
        int[] specificAppSizes = (int[])config.get("specificAppSizes");
        numberOfApps = (int) (numberOfApps / specificAppSizes.length);
        NormalDistribution normCT = new NormalDistribution(rand, meanPairConstraint, 1);
        int a=0;
        for(int i=1;i<=numberOfApps;i++) {
            for (int funcsInApp : specificAppSizes){
                a++;
                Set<Integer> usedFuncs = new HashSet<>();
                int p = 1;
                int f1 = 0;
                f1 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
                usedFuncs.add(f1);
                for (int ind=1; ind < funcsInApp; ind++) {
                    int f2 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
                    while (usedFuncs.contains(f2))
                        f2 = funcList.get(sampleZipf(rand, funcList.size()) - 1);
                    usedFuncs.add(f2);
                    int CT;
                    if (normalDistLatencyConstAppSize)
                        CT = (int) normCT.sample();
                    else
                        CT = (int) meanPairConstraint;
                    String[] app = new String[4]; //appID,pairID,pair,pairCT
                    app[0] = String.valueOf(a);
                    app[1] = String.valueOf(p);
                    app[2] = f1 + "-" + f2;
                    app[3] = String.valueOf(CT);

                    App app1 = new App(apps, functions, app);
                    apps.put(app1.getId(), app1);
                    f1 = f2; //for next pair
                    p++;
                }
            }
        }
        return apps;
    }
    public int getNumOfPairs() {
        return numOfPairs;
    }

    public void addToAppFunctions(int f) {
        appFunctions.add(f);
    }

    public void addZeroFunctions(HashMap<Integer,Function> functions, HashMap<String,Object> config){
        boolean nonSplitMode = (Boolean) config.get("nonSplitMode");
        numOfPairs = pairs.size();
/*        if (nonSplitMode) {
            int meanLinkDelay = (int) config.get("delay");
            int numOfRealFuncs = functions.size()-2;
            createVirtualPairs(numOfRealFuncs, meanLinkDelay);
        }*/
        //From client to first function
        int f = pairs.get(1).getValue0();
        pairs.put(0,new Pair(0,f));
        pairCTs.put(0,0);

        //From last function to client
        int g = pairs.get(numOfPairs).getValue1();
        pairs.put(numOfPairs+1,new Pair(g,0));
        pairCTs.put(numOfPairs+1,0);
    }

    /**
     * Add 0 proc function at first and last locations to apply pair timings constraints from/to user
     */
    private void createVirtualPairs(int numOfRealFuncs, int meanLinkDelay){
        int nonlimitingCT = meanLinkDelay*10;
        int f = pairs.get(1).getValue0();
        int vf = numOfRealFuncs+1;
        addToAppFunctions(vf);
        App.incrementKeys(pairs);
        App.incrementKeys(pairCTs);
        pairs.put(1,new Pair(vf,f));
        pairCTs.put(1,nonlimitingCT);
        numOfPairs++;

        int g = pairs.get(numOfPairs).getValue1();
        int vg = numOfRealFuncs+2;
        addToAppFunctions(vg);
        numOfPairs++;
        pairs.put(numOfPairs,new Pair(g,vg));
        pairCTs.put(numOfPairs,nonlimitingCT);
    }

    private static void createVirtualFunctions(HashMap<Integer,Function> functions){
        int vf = functions.size()+1;
        String[] function = new String[3];
        function[0] = String.valueOf(vf);
        function[1] = String.valueOf(0);
        functions.put(vf, new Function(function));
    }
    /**
     * Check if there is a direct hop between two functions
     * @return '1' if these are subsequent functions
     */
    public int checkPair(int f, int g){
        return pairs.containsValue(new Pair(f,g)) ? 1 : 0;
    }
    public static <V> void incrementKeys(HashMap<Integer, V> map) {
        List<Integer> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys, Collections.reverseOrder());
        for (int key : keys) {
            V value = map.get(key);
            map.remove(key);
            map.put(key + 1, value);
        }
    }


}
