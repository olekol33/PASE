package Inputs;

import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class DC implements Cloneable{
    private int id;
    private int x;
    private int y;
    private int capacity;
    private int originalCapacity;

    public int getDynamicSize() {
        return dynamicSize;
    }

    private int dynamicSize;
    private int procPower;
    private int stateCost;
    private List<Integer> neigbors;
    private HashMap<Integer,Boolean> existingFunc;

    public HashMap<Pair<Integer, Integer>, Boolean> getExistingState() {
        return existingState;
    }

    /**
     * Returns 1 if state not existed previously
     * @param r request ID
     * @param f function
     */
    public int getExistingSingleState(int r, int f) {
            return existingState.get(Pair.with(r,f))? 0 : 1; //return 1 if false on existing state
    }


    public boolean containsExistingState(int r, int f) {
        return existingState.containsKey(Pair.with(r,f));
    }

    public void addExistingState(int r, int f) {
        existingState.put(Pair.with(r,f),false);
    }
    public void setExistingSingleState(int r, int f, boolean state) {
        existingState.put(Pair.with(r,f),state);
    }

    public void removeStates(int r, Set<Integer> functions) {
        for(int f:functions) {
            if(existingState.containsKey(Pair.with(r, f)))
                existingState.remove(Pair.with(r, f));
        }
    }

    public int getOriginalCapacity() {
        return originalCapacity;
    }

    public void setOriginalCapacity(int originalCapacity) {
        this.originalCapacity = originalCapacity;
    }

    public static boolean areDCsInRange(DC dc1, DC dc2, int range){
        if ((Math.abs(dc1.getX() - dc2.getX()) <= range) && (Math.abs(dc1.getY() - dc2.getY()) <= range)) {
            return true;
        }
        else
            return false;
    }

    private HashMap<Pair<Integer,Integer>,Boolean> existingState; //<r,f>

    private static final int ID=0;
    private static final int X=1;
    private static final int Y=2;
    private static final int CAPACITY=3;
    private static final int PROC_POWER=4;
    private static final int STATE_COST=5;

    public Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }

    public int getId() {
        return id;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    /** Add delta (positive or negative) to existing capacity
     *
     * @param delta
     * @param rID
     * @param fID
     */
    public void updateCapacity(int delta, int rID, int fID) {
        this.capacity += delta;
/*        if (delta<0)
            System.out.println("PLACED -- Request: " + String.valueOf(rID) + " - Function: " + String.valueOf(fID));
        else
            System.out.println("REMOVED -- Request: " + String.valueOf(rID) + " - Function: " + String.valueOf(fID));*/

        if(this.capacity<0)
            throw new IllegalStateException("Negative capacity");
        if(this.capacity>this.originalCapacity)
            throw new IllegalStateException("Illegal capacity increase");
    }

    /** Add delta (positive or negative) to existing dynamic size used
     *
     * @param delta
     */
    public void updateDynamicSize(int delta) {
        this.dynamicSize += delta;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getProcPower() {
        return procPower;
    }

    public int getExistingFunc(int f) {
        return existingFunc.get(f)? 0 : 1;
    }

    public void setExistingState(HashMap<Pair<Integer, Integer>, Boolean> existingState) {
        this.existingState = existingState;
    }

    public void setExistingFunc(HashMap<Integer, Boolean> existingFunc) {
        this.existingFunc = existingFunc;
    }

    public void updateExistingFunc(int f, boolean existing) {
        existingFunc.put(f,existing);
    }

    public HashMap<Integer, Boolean> getExistingFunc() {
        return existingFunc;
    }
    public void setStateCost(int stateCost) {
        this.stateCost = stateCost;
    }

    public int getStateCost() {
        return stateCost;
    }

    public List<Integer> getNeigbors() {
        return neigbors;
    }

    public void setNeigbors(List<Integer> neigbors) {
        this.neigbors = neigbors;
    }

    public DC(String[] dc, int stateCost, int capacity){
        id = Integer.parseInt(dc[ID]);
        x = Integer.parseInt(dc[X]);
        y = Integer.parseInt(dc[Y]);
        procPower = Integer.parseInt(dc[PROC_POWER]);
        if (capacity>0)
            this.capacity = capacity;
        else
            this.capacity = Integer.parseInt(dc[CAPACITY]);
        if (stateCost>=0)
            this.stateCost = stateCost;
        else
            this.stateCost = Integer.parseInt(dc[STATE_COST]);
        existingState = new HashMap<>();
        originalCapacity = this.capacity;
        neigbors = new ArrayList<>();
        dynamicSize =0;
    }

    public DC(int id, int x, int y, int capacity, int procPower, int stateCost, int originalCapacity, List<Integer> neigbors){
        this.id = id;
        this.x = x;
        this.y = y;
        this.capacity = capacity;
        this.procPower = procPower;
        this.stateCost = stateCost;
        this.originalCapacity = originalCapacity;
        this.neigbors = neigbors;
        existingState = new HashMap<>();
        dynamicSize =0;
    }

    public static HashMap<Integer,Boolean> genExistingFunctionsSet(Set<Integer> functions){
        HashMap<Integer,Boolean> existing = new HashMap<>();
        for (Integer func : functions) {
            existing.put(func,false);
        }
        return existing;
    }

}
