package diskmgr;

public class PCounter {
    public static int rcounter;
    public static int wcounter;
    //initialize is called when the DB is first initialized
    public static void initialize() {
        rcounter =0;
        wcounter =0;
    }
    //readcounter is incremented
    public static void readIncrement() {
        rcounter++;
    }
    //write counter is incremented
    public static void writeIncrement() {
        wcounter++;
    }
}

