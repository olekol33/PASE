/**For request for this VNF contains location of covering instance and whether it's dynamic (need to launch) */
class vnfPlace{
    int dc;
    Boolean dynamic;

    public vnfPlace(int dc, Boolean dynamic) {
        this.dc = dc;
        this.dynamic = dynamic;
    }

    public int getDc() {
        return dc;
    }

    public Boolean isDynamic() {
        return dynamic;
    }
}