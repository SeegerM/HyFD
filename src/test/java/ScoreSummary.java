public class ScoreSummary {
    int tp;
    int fp;
    int fn;
    double f1;

    public ScoreSummary(int tp, int fp, int fn, double f1){
        this.tp = tp;
        this.fp = fp;
        this.fn = fn;
        this.f1 = f1;
    }

    public int tp() {
        return tp;
    }

    public int fp() {
        return fp;
    }


    public double f1() {
        return f1;
    }


    public int fn() {
        return fn;
    }
}
