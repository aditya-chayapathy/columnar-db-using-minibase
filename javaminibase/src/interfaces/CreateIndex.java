package interfaces;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.SystemDefs;

import static global.GlobalConst.NUMBUF;

public class CreateIndex {

    public static void main(String[] args) throws Exception {
        // Query Skeleton: COLUMNDB COLUMNARFILE COLUMNNAME INDEXTYPE
        // Example Query: testColumnDB columnarTable columnName BITMAP|BTREE
        String columnDB = args[0];
        String columnarFile = args[1];
        String columnName = args[2];
        String indexType = args[3];

        String dbpath = InterfaceUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, NUMBUF, "Clock");

        runInterface(columnarFile, columnName, indexType);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String columnName, String indexType) throws Exception {

        Columnarfile cf = new Columnarfile(columnarFile);
        int colno = cf.getAttributePosition(columnName);

        if (indexType.equals("BITMAP")) {
            cf.createAllBitMapIndexForColumn(colno);
        } else {
            cf.createBTreeIndex(colno);
        }
        cf.close();

        System.out.println(indexType + " created successfully on "+columnarFile+"."+columnName);
    }
}
