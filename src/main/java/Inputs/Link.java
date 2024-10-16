package Inputs;

import org.javatuples.Pair;

import java.util.HashMap;

public class Link implements Cloneable {
    private int node1;

    public void setNode1(int node1) {
        this.node1 = node1;
    }

    public void setNode2(int node2) {
        this.node2 = node2;
    }

    private int node2;
    private int delay;
    private int bw;
    private int originalBW;

    private static final int SRC = 0;
    private static final int DST = 1;
    private static final int DELAY = 2;
    private static final int BW = 3;

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public int getNode1() {
        return node1;
    }

    public int getNode2() {
        return node2;
    }

    public int getDelay() {
        return delay;
    }

    public void setBw(int bw) {
        this.bw = bw;
    }

    public void updateBW(int bw) {
        this.bw += bw;
        if(this.bw<0)
            throw new IllegalStateException("BW<0");
        if(this.bw>this.originalBW)
            throw new IllegalStateException("BW> originalBW");
    }

    public int getBw() {
        return bw;
    }

    public int getOriginalBW() {
        return originalBW;
    }

    public Link(String[] link, HashMap<Integer, DC> dcs, int range) {
        node1 = Integer.parseInt(link[SRC]);
        node2 = Integer.parseInt(link[DST]);
        delay = Integer.parseInt(link[DELAY]);
        if (checkIfLinkWithinRange(dcs, node1, node2,range))
            bw = Integer.parseInt(link[BW]);
        else {
            bw = 0;
            delay = 0;
        }
        originalBW = bw;
    }

    public Link(int node1, int node2, int delay, int bw) {
        this.node1 = node1;
        this.node2 = node2;
        this.delay = delay;
        this.bw = bw;
        this.originalBW = bw;
    }

    public Link(int node1, int node2, boolean zeroLink) {
        if (zeroLink) {
            this.node1 = node1;
            this.node2 = node2;
//            delay = Integer.MAX_VALUE;
            delay = 0;
            bw = 0;
        }

    }

    /**
     * Check if length of link is within allowed range
     *
     * @param dcs collection of all datacenters
     */
    public static boolean checkIfLinkWithinRange(HashMap<Integer, DC> dcs, int d1, int d2, int range) {

        DC dc1 = dcs.get(d1);
        DC dc2 = dcs.get(d2);
        if ((Math.abs(dc1.getX() - dc2.getX()) > range) || (Math.abs(dc1.getY() - dc2.getY()) > range)) {
            return false;
        } else
            return true;
    }

    /**
     * Generate HashMap of links using data center dataset
     * If link not within range (predefined value), bw and delay are 0
     * @param config input config
     * @param dcs datacenters
     * @param delay
     * @param bw
     */
    public static void genLinks(HashMap<Pair<Integer, Integer>, Link> links, HashMap<String, Object> config,
                                HashMap<Integer, DC> dcs, int delay, int bw) {

        if(dcs.size()==0)
            throw new IllegalStateException("No dcs input");
        for (int d1 = 0; d1 < dcs.size(); d1++) {
            for (int d2 = 0; d2 < dcs.size(); d2++) {
                if(d1==d2)
                    continue;
                String[] link = new String[4]; //srcNode,dstNode,delay,bw
                link[SRC] = String.valueOf(d1);
                link[DST] = String.valueOf(d2);
                link[DELAY] = String.valueOf(delay);
                link[BW] = String.valueOf(bw);

                Link link1 = new Link(link, dcs, (int) config.get("allowedRange"));
                links.put(new Pair<>(link1.getNode1(), link1.getNode2()), link1);
            }
        }
    }

    public static boolean[][] collectDcsWithinRange(HashMap<Integer, DC> dcs, int range){
        boolean[][] dcsInRange = new boolean[dcs.size()][dcs.size()];
        for (int d1 = 0; d1 < dcs.size(); d1++) {
            for (int d2 = 0; d2 < dcs.size(); d2++) {
                if (checkIfLinkWithinRange(dcs, d1, d2, range))
                    dcsInRange[d1][d2] = true;
            }
        }
        return dcsInRange;
    }

}
