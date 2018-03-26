package tests;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.IOException;

import static global.GlobalConst.NUMBUF;

class IndexTestDriver extends TestDriver {

    private  int numPages = 1000;
    private String dbName;
    private String colFilename;
    private String colName;
    private String indexType;
    private int numCols;
    AttrType[] types;
    short[] sizes;

    //private boolean delete = true;
    public IndexTestDriver() {
        super("BatchInsert");
    }

    public IndexTestDriver(String columnName, String columnDBName, String columnarFileName, String indexTypeVal) {
        super(columnDBName);
        colName = columnName;
        dbName = columnDBName;
        colFilename = columnarFileName;
        indexType = indexTypeVal;
    }

    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + dbpath+"\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 0, NUMBUF, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = isUnix()? "/bin/rm -rf " : "cmd /c del /f ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        boolean _pass = runAllTests();
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        }catch (Exception e) {
            System.out.println("coming from here");
            System.err.println("error: " + e);
        }


        System.out.println("Reads: "+PCounter.rcounter);
        System.out.println("Writes: "+PCounter.wcounter);
        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;

    }

    protected boolean test1(){

        try {
            Columnarfile cf = new Columnarfile(colFilename);
            int colno = cf.getAttributePosition(colName);

            if (indexType.equals("BITMAP")) {
                //cf.createBitMapIndex(val, value);
                cf.createAllBitMapIndexForColumn(colno);
            }
            else {
                cf.createBTreeIndex((short)colno);
            }
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return true;
    }

    protected String testName() {

        return "Index ";
    }
}

public class IndexCreateTest {

    public static String columnDBName;
    public static String columnarFileName;
    public static String columnName;
    public static String indexType;

    public static void runTests() {

        IndexTestDriver cd = new IndexTestDriver(columnName, columnDBName, columnarFileName, indexType);
        cd.runTests();
    }

    public static void main(String[] argvs) {

        try {
            IndexCreateTest indexTest = new IndexCreateTest();

            columnName = argvs[2];
            columnDBName = argvs[0];
            columnarFileName = argvs[1];
            indexType = argvs[3];
            indexTest.runTests();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}
