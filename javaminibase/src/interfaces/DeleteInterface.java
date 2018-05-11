package interfaces;


import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.Tuple;
import iterator.*;

public class DeleteInterface {

    private static String FILESCAN = "FILE";
    private static String COLUMNSCAN = "COLUMN";
    private static String BITMAPSCAN = "BITMAP";
    private static String BTREESCAN = "BTREE";

    public static void main(String args[]) throws Exception {
        // Query Skeleton: COLUMNDB COLUMNFILE PROJECTION OTHERCONST SCANCOLS [SCANTYPE] [SCANCONST] TARGETCOLUMNS NUMBUF
        // Example Query: testColumnDB columnarTable A,B,C "C = 5" A,B [BTREE,BITMAP] "(A = 5 v A = 6),(B > 7)" A,B,C 100
        // In case no constraints need to be applied, pass "" as input.
        String columnDB = args[0];
        String columnarFile = args[1];
        String[] projection = args[2].split(",");
        String otherConstraints = args[3];
        String[] scanColumns = args[4].split(",");
        String[] scanTypes = args[5].split(",");
        String[] scanConstraints = args[6].split(",");
        String[] targetColumns = args[7].split(",");
        Integer bufferSize = Integer.parseInt(args[8]);
        String purge = args[9];
        Integer sortmem = Integer.parseInt(args[10]);

        String dbpath = InterfaceUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        runInterface(columnarFile, projection, otherConstraints, scanColumns, scanTypes, scanConstraints, targetColumns, purge, sortmem);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String[] projection, String otherConstraints, String[] scanColumns, String[] scanTypes, String[] scanConstraints, String[] targetColumns, String purge, int sortmem) throws Exception {

        Columnarfile cf = new Columnarfile(columnarFile);

        AttrType[] opAttr = new AttrType[projection.length];
        FldSpec[] projectionList = new FldSpec[projection.length];
        for (int i = 0; i < projection.length; i++) {
            String attribute = InterfaceUtils.getAttributeName(projection[i]);
            projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), InterfaceUtils.getColumnPositionInTargets(attribute, targetColumns) + 1);
            opAttr[i] = new AttrType(cf.getAttrtypeforcolumn(cf.getAttributePosition(attribute)).attrType);
        }

        int[] scanCols = new int[scanColumns.length];
        for (int i = 0; i < scanColumns.length; i++) {
            if (!scanColumns[i].equals("")) {
                String attribute = InterfaceUtils.getAttributeName(scanColumns[i]);
                scanCols[i] = cf.getAttributePosition(attribute);
            }
        }

        short[] targets = new short[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++) {
            String attribute = InterfaceUtils.getAttributeName(targetColumns[i]);
            targets[i] = (short) cf.getAttributePosition(attribute);
        }

        CondExpr[] otherConstraint = InterfaceUtils.processRawConditionExpression(otherConstraints, targetColumns);

        CondExpr[][] scanConstraint = new CondExpr[scanTypes.length][1];

        for (int i = 0; i < scanTypes.length; i++) {
            scanConstraint[i] = InterfaceUtils.processRawConditionExpression(scanConstraints[i]);
        }
        cf.close();
        Iterator it = null;
        int cnt = 0;
        try {
            if (scanTypes[0].equals(FILESCAN)) {
                ColumnarFileScan cfs;
                cfs = new ColumnarFileScan(columnarFile, projectionList, targets, otherConstraint);
                Boolean deleted = true;
                while (deleted) {
                    deleted = cfs.delete_next();

                    if (deleted == false) {
                        break;
                    }
                    cnt++;
                }
                cfs.close();
            } else if (scanTypes[0].equals(COLUMNSCAN)) {
                ColumnarColumnScan ccs;
                ccs = new ColumnarColumnScan(columnarFile, scanCols[0], projectionList, targets, scanConstraint[0], otherConstraint);
                Boolean deleted = true;
                while (deleted) {
                    deleted = ccs.delete_next();

                    if (deleted == false) {
                        break;
                    }
                    cnt++;
                }
                ccs.close();

            } else if (scanTypes[0].equals(BITMAPSCAN) || scanTypes[0].equals(BTREESCAN)) {
                IndexType[] indexType = new IndexType[scanTypes.length];
                for (int i = 0; i < scanTypes.length; i++) {
                    if (scanTypes[i].equals(BITMAPSCAN))
                        indexType[i] = new IndexType(IndexType.BitMapIndex);
                    else if (scanTypes[i].equals(BTREESCAN))
                        indexType[i] = new IndexType(IndexType.B_Index);
                    else
                        throw new Exception("Scan type <" + scanTypes[i] + "> not recognized.");
                }
                ColumnarIndexScan cis;
                cis = new ColumnarIndexScan(columnarFile, scanCols, indexType, scanConstraint, otherConstraint, false, targets, projectionList, sortmem);
                Boolean deleted = true;
                while (deleted) {
                    deleted = cis.delete_next();

                    if (deleted == false) {
                        break;
                    }
                    cnt++;
                }

                cis.close();

            } else
                throw new Exception("Scan type <" + scanTypes[0] + "> not recognized.");


            if(purge.equals("PURGE")){
                cf.purgeAllDeletedTuples();
            }
            cf.close();

            System.out.println();
            System.out.println(cnt + " tuples selected");
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            it.close();
        }
    }
}

