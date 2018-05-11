package tests;
/*
import bitmap.BM;
import bitmap.BitMapFile;
import columnar.*;
import diskmgr.PCounter;
import global.*;
import heap.Scan;
import heap.Tuple;
import iterator.ColumnarIndexScan;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import static global.GlobalConst.NUMBUF;

class ColumnarDriver extends TestDriver {

    private  int numPages = 10000;
    boolean skip = true;
    //private boolean delete = true;
    public ColumnarDriver() {
        super("cmtest");
    }

    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, numPages, NUMBUF, "Clock");

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


        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();


        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        boolean _pass = runAllTests();
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        }catch (Exception e) {
            System.err.println("error: " + e);
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;

    }

    protected boolean test1(){
        if(numPages == 0)
            return true;
        try {
            String name = "file1";
            int numColumns = 3;
            AttrType[] types = new AttrType[numColumns];
            types[0] = new AttrType(AttrType.attrInteger);
            types[1] = new AttrType(AttrType.attrReal);
            types[2] = new AttrType(AttrType.attrString);
            short[] sizes = new short[1];
            sizes[0] = 20;
            System.out.println("Creating columnar " + name);
            String[] attrNames = {"Attr1", "Attr2","Attr3"};
            Columnarfile cf = new Columnarfile(name, numColumns, types, sizes, attrNames);

            System.out.println("Inserting columns..");
            for(int i = 0; i < 20; i++){

                Tuple t = new Tuple();
                t.setHdr((short)3, types, sizes);
                int s = t.size();
                t = new Tuple(s);
                t.setHdr((short)3, types, sizes);
                t.setIntFld(1,i%5);
                t.setFloFld(2, (float)(i*1.1));
                t.setStrFld(3, "A"+i);
                TID tid =  cf.insertTuple(t.getTupleByteArray());
                System.out.println(i+","+(i*1.1)+",A"+i);
                t = cf.getTuple(tid);
                System.out.println(i+","+(i*1.1)+",A"+i);
                ValueClass v = cf.getValue(tid,2);
                System.out.println(v.getValue());
            }
            cf.close();
            System.out.println("Reads: "+PCounter.rcounter);
            System.out.println("Writes: "+PCounter.wcounter);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    protected boolean test2() {

        if(numPages == 0)
            return true;
        String name = "file1";
        System.out.println("Opening columnar " + name);

        try {
            Columnarfile cf = new Columnarfile(name);
            System.out.println("File contains " + cf.getTupleCnt()+" tuples.");
            Scan sc = cf.openColumnScan(0);
            RID rr = new RID();
            Tuple tt = sc.getNext(rr);
            while (tt != null){
                System.out.println(Convert.getIntValue(0, tt.getTupleByteArray()));
                rr = new RID();
                tt = sc.getNext(rr);
            }
            sc.closescan();

            TupleScan scan = cf.openTupleScan();

            TID tid = new TID();
            Tuple t = scan.getNext(tid);
            while (t != null){
                System.out.println(t.getIntFld(1)+","+t.getFloFld(2)+","+t.getStrFld(3));
                //t.setIntFld(1, 99);
                //cf.updateTuple(tid, t);
                //t.setStrFld(3, "ABC");
                //cf.updateColumnofTuple(tid, t, 2);
                t = scan.getNext(tid);
            }
            scan.closetuplescan();

            scan = cf.openTupleScan();

            tid = new TID();
            t = scan.getNext(tid);
            while (t != null){
                System.out.println(t.getIntFld(1)+","+t.getFloFld(2)+","+t.getStrFld(3));
                t = scan.getNext(tid);
            }
            scan.closetuplescan();
            cf.close();
            System.out.println("Reads: "+PCounter.rcounter);
            System.out.println("Writes: "+PCounter.wcounter);

        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    protected boolean test3() {

        if(numPages == 0)
            return true;
        try{

            AttrType[] Stypes = new AttrType[3];
            Stypes[0] = new AttrType(AttrType.attrInteger);
            //Stypes[1] = new AttrType(AttrType.attrReal);
            Stypes[1] = new AttrType(AttrType.attrString);

            short[] Ssizes = new short[1];
            Ssizes[0] = 20;

            FldSpec[] Sprojection = new FldSpec[1];
            Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
            //Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 1);


            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGT);
            expr[0].next = null;
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.integer = 10;
            expr[1] = new CondExpr();
            expr[1] = null;

            short[] in_cols = new short[2];
            in_cols[0] = 0;
            in_cols[1] = 2;

            ColumnarFileScan am = new ColumnarFileScan("file1", Stypes, Ssizes,
                    (short) 2, (short) 1,
                    Sprojection, expr, in_cols);

            Tuple t = am.get_next();
            while (t != null){
                System.out.println(t.getStrFld(1));
                t = am.get_next();
            }
            am.close();

            short[] targetedCols = new short[1];

            targetedCols[0] = 0;
            //targetedCols[1] = 2;

            Columnarfile cf = new Columnarfile("file1");
            cf.createBTreeIndex(0);

            IndexType[] indexTypes = new IndexType[1];
            indexTypes[0] = new IndexType(1);

            int[] columnNos = new int[1];
            columnNos[0] = 0;

            CondExpr[][] indexExps = new CondExpr[1][1];
            indexExps[0]= expr;

            ColumnarIndexScan cis = new ColumnarIndexScan("file1", columnNos,indexTypes,indexExps, null, true, targetedCols, null);

            t = cis.get_next();
            while (t != null){
                System.out.println(t.getIntFld(1));
                t = cis.get_next();
            }
            cis.close();
            cf.close();
            System.out.println("Reads: "+PCounter.rcounter);
            System.out.println("Writes: "+PCounter.wcounter);

        } catch (Exception e){
            e.printStackTrace();
            return false;
        }

        return true;
    }

    // equality search using bitMaps
    protected boolean test4()  {
        if(numPages == 0)
            return true;
        System.out.println("####################################");
        System.out.println("#### T E S T 4 ####################");
        System.out.println("####################################");

        try {

            String name = "file5";
            int numColumns = 5;
            AttrType[] types = new AttrType[numColumns];
            types[0] = new AttrType(AttrType.attrInteger);
            types[1] = new AttrType(AttrType.attrInteger);
            types[2] = new AttrType(AttrType.attrString);
            types[3] = new AttrType(AttrType.attrString);
            types[4] = new AttrType(AttrType.attrString);

            short[] sizes = new short[3];
            sizes[0] = 20;
            sizes[1] = 20;
            sizes[2] = 20;
            String[] attrNames = {"Attr1", "Attr2","Attr3","Attr4", "Attr5"};
            Columnarfile cf = new Columnarfile(name, numColumns, types, sizes, attrNames);

            for (int i = 0; i < 100; i++) {
                Tuple t = new Tuple();
                t.setHdr((short) 5, types, sizes);
                int s = t.size();
                t = new Tuple(s);
                t.setHdr((short) 5, types, sizes);
                //if(i%2 == 0) {
                    t.setIntFld(1, i%10);
                //} else {
                //    t.setIntFld(1, 5);
                //}

                t.setIntFld(2, 6);
                String asd = i % 5 == 0 ? "C" : i % 3 == 0? "B" : i % 2 == 0? "A" : "D";
                t.setStrFld(3, asd);
                t.setStrFld(4, "B" + i);
                t.setStrFld(5, "C" + i);
                cf.insertTuple(t.getTupleByteArray());
            }

            System.out.println("Reads: "+PCounter.rcounter);
            System.out.println("Writes: "+PCounter.wcounter);
            cf.createAllBitMapIndexForColumn(0);

            BitMapFile bitMapFile = new BitMapFile(cf.getBMName(0,new ValueInt<>(4)));
            BM.printBitMap(bitMapFile.getHeaderPage());

            short[] targetedCols = new short[2];

            targetedCols[0] = 0;
            targetedCols[1] = 2;

            IndexType indexType = new IndexType(3);

            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGT);
            expr[0].next = null;
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.integer = 7;
            expr[1] = new CondExpr();
            expr[1] = null;

            CondExpr[] selects = new CondExpr[2];
            selects[0] = new CondExpr();
            selects[0].op = new AttrOperator(AttrOperator.aopEQ);
            selects[0].next = new CondExpr();
            selects[0].type1 = new AttrType(AttrType.attrSymbol);
            selects[0].type2 = new AttrType(AttrType.attrString);
            selects[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            selects[0].operand2.string = "A";
            selects[1] = new CondExpr();
            selects[1] = null;
            selects[0].next.op = new AttrOperator(AttrOperator.aopEQ);
            selects[0].next.next = null;
            selects[0].next.type1 = new AttrType(AttrType.attrSymbol);
            selects[0].next.type2 = new AttrType(AttrType.attrString);
            selects[0].next.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            selects[0].next.operand2.string = "B";

            IndexType[] indexTypes = new IndexType[1];
            indexTypes[0] = new IndexType(1);

            int[] columnNos = new int[1];
            columnNos[0] = 0;

            CondExpr[][] indexExps = new CondExpr[1][1];
            indexExps[0]= expr;

            ColumnarIndexScan columnIndexScan = new ColumnarIndexScan(cf.getColumnarFileName(), columnNos, indexTypes, indexExps, selects, false, targetedCols, null);


            System.out.println("Select 0,2 where 0 > 7 and (2 = 'A' || 2 = 'B')");
            System.out.println("0 > 7 is handled at bitmap level");
            System.out.println("Starting Index Scan");
            Tuple tuples = columnIndexScan.get_next();


            while (tuples != null){
                System.out.print(tuples.getIntFld(1));
                System.out.print("  ");
                System.out.print(tuples.getStrFld(2));
                System.out.println();
                //System.out.println(tuples.getStrFld(2));
                tuples = columnIndexScan.get_next();
            }
            columnIndexScan.close();
            cf.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        System.out.println("Reads: "+  PCounter.rcounter);
        System.out.println("Writes: "+ PCounter.wcounter);
        return true;
    }

    protected boolean test5() {
        if(numPages == 0)
            return true;
        System.out.println("####################################");
        System.out.println("#### T E S T 5 ####################");
        System.out.println("####################################");
        try {
            String name = "file5";
            int numColumns = 5;
            AttrType[] types = new AttrType[numColumns];
            types[0] = new AttrType(AttrType.attrInteger);
            types[1] = new AttrType(AttrType.attrInteger);
            types[2] = new AttrType(AttrType.attrString);
            types[3] = new AttrType(AttrType.attrString);
            types[4] = new AttrType(AttrType.attrString);

            short[] sizes = new short[3];
            sizes[0] = 20;
            sizes[1] = 20;
            sizes[2] = 20;
            String[] attrNames = {"Attr1", "Attr2","Attr3","Attr4", "Attr5"};
            Columnarfile cf = new Columnarfile(name, numColumns, types, sizes, attrNames);

            for (int i = 0; i < 100; i++) {
                Tuple t = new Tuple();
                t.setHdr((short) 5, types, sizes);
                int s = t.size();
                t = new Tuple(s);
                t.setHdr((short) 5, types, sizes);
                if(i%2 == 0) {
                    t.setIntFld(1, 4);
                } else {
                    t.setIntFld(1, 5);
                }

                t.setIntFld(2, 6);
                t.setStrFld(3, "A" + i);
                t.setStrFld(4, "B" + i);
                t.setStrFld(5, "C" + i);
                cf.insertTuple(t.getTupleByteArray());
            }
            cf.createAllBitMapIndexForColumn(0);
            BitMapFile bitMapFile = new BitMapFile("bitmap-file-5", cf, 1, new ValueInt(4));
            TupleScan scan = cf.openTupleScan();
            TID tid = new TID();
            Tuple t = scan.getNext(tid);
            Integer count = 1;
            while (t != null) {
                if (t.getIntFld(1) == 4) {
                    bitMapFile.insert(count);
                } else {
                    bitMapFile.delete(count);
                }
                count++;
                t = scan.getNext(tid);
            }
            scan.closetuplescan();

            short[] targetedCols = new short[3];

            targetedCols[0] = 1;
            targetedCols[1] = 2;
            targetedCols[2] = 3;

            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGT);
            expr[0].next = null;
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            expr[0].operand2.integer = 4;
            expr[1] = new CondExpr();
            expr[1] = null;

            AttrType[] attrTypes = new AttrType[1];
            attrTypes[0] = new AttrType(AttrType.attrInteger);

            IndexType[] indexTypes = new IndexType[1];
            indexTypes[0] = new IndexType(3);

            int[] columnNos = new int[1];
            columnNos[0] = 0;

            CondExpr[][] indexExps = new CondExpr[1][1];
            indexExps[0]= expr;

            ColumnarIndexScan columnIndexScan = new ColumnarIndexScan(name, columnNos, indexTypes, indexExps,null,false, targetedCols, null);


            System.out.println("Starting Index Scan");
            Tuple tuples = columnIndexScan.get_next();

            while (tuples != null){
                System.out.println(tuples.getIntFld(1));
                tuples = columnIndexScan.get_next();
            }

            columnIndexScan.close();
            cf.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        System.out.println("Reads: "+  PCounter.rcounter);
        System.out.println("Writes: "+ PCounter.wcounter);
        return true;
    }

    //test case for markTuple as Deleted
    protected boolean test6() {

        System.out.println("####################################");
        System.out.println("#### T E S T 6 ####################");
        System.out.println("####################################");

        if(numPages == 0)
            return true;
        try {
            String name = "file6";
            int numColumns = 3;
            AttrType[] types = new AttrType[numColumns];
            types[0] = new AttrType(AttrType.attrInteger);
            types[1] = new AttrType(AttrType.attrString);
            types[2] = new AttrType(AttrType.attrString);
            short[] sizes = new short[2];
            sizes[0] = 20;
            sizes[1] = 20;
            System.out.println("Creating columnar " + name);
            String[] attrNames = {"Attr1", "Attr2","Attr3"};
            Columnarfile cf = new Columnarfile(name, numColumns, types, sizes, attrNames);

            TID tidToMarkedAsDeleted = null;
            TID tid = null;
            System.out.println("Inserting columns..");
            for(int i = 0; i < 50; i++){

                Tuple t = new Tuple();
                t.setHdr((short)3, types, sizes);
                int s = t.size();
                t = new Tuple(s);
                t.setHdr((short)3, types, sizes);
                if(i%2 == 0) {
                    t.setIntFld(1,4);
                } else {
                    t.setIntFld(1,i);
                }
                t.setStrFld(2, "A"+i);
                t.setStrFld(3, "B"+i);

                if(i == 12) {
                     tidToMarkedAsDeleted =  cf.insertTuple(t.getTupleByteArray());
                    System.out.println(tidToMarkedAsDeleted.getPosition());

                } else {
                     tid =  cf.insertTuple(t.getTupleByteArray());
                }
            }

            cf.createBitMapIndex(0, new ValueInt<>(4));
            BitMapFile bitMap;

            bitMap = cf.getBMIndex(cf.getBMName(0, new ValueInt<>(4)));
            System.out.println("####################################");
            System.out.println("#### Bit map before deletion ####################");
            System.out.println("####################################");

            BM.printBitMap(bitMap.getHeaderPage());


            cf.markTupleDeleted(tidToMarkedAsDeleted);
            bitMap = cf.getBMIndex(cf.getBMName(0, new ValueInt<>(4)));


            System.out.println("####################################");
            System.out.println("#### Bit map after deletion ####################");
            System.out.println("####################################");
            BM.printBitMap(bitMap.getHeaderPage());
            cf.close();
            System.out.println("Reads: "+PCounter.rcounter);
            System.out.println("Writes: "+PCounter.wcounter);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }


    // test 7 is for purgeAllDeletedTuples
    protected boolean test7() {

        System.out.println("####################################");
        System.out.println("#### T E S T 7 ####################");
        System.out.println("####################################");

        if(numPages == 0)
            return true;
        try {
            String name = "file7";
            int numColumns = 3;
            AttrType[] types = new AttrType[numColumns];
            types[0] = new AttrType(AttrType.attrInteger);
            types[1] = new AttrType(AttrType.attrString);
            types[2] = new AttrType(AttrType.attrString);
            short[] sizes = new short[2];
            sizes[0] = 20;
            sizes[1] = 20;
            System.out.println("Creating columnar " + name);
            String[] attrNames = {"Attr1", "Attr2","Attr3"};
            Columnarfile cf = new Columnarfile(name, numColumns, types, sizes, attrNames);

            TID tidToMarkedAsDeleted = null;
            TID tid = null;
            System.out.println("Inserting columns..");
            for(int i = 0; i < 50; i++){

                Tuple t = new Tuple();
                t.setHdr((short)3, types, sizes);
                int s = t.size();
                t = new Tuple(s);
                t.setHdr((short)3, types, sizes);

                t.setIntFld(1,i);
                t.setStrFld(2, "A"+i);
                t.setStrFld(3, "B"+i);

                if(i % 3 ==0) {
                    tidToMarkedAsDeleted =  cf.insertTuple(t.getTupleByteArray());
                    cf.markTupleDeleted(tidToMarkedAsDeleted);
                } else {
                    tid =  cf.insertTuple(t.getTupleByteArray());
                }
                System.out.println(i);
            }

            cf.purgeAllDeletedTuples();

            try {
                System.out.println("File contains " + cf.getTupleCnt()+" tuples.");
                TupleScan scan = cf.openTupleScan();

                tid = new TID();
                Tuple t = scan.getNext(tid);
                while (t != null){
                    System.out.println(t.getIntFld(1)+","+t.getStrFld(2)+","+t.getStrFld(3));
                    t = scan.getNext(tid);
                }
                scan.closetuplescan();


            } catch (Exception e){
                e.printStackTrace();
                return false;
            }
            cf.close();
            System.out.println("Reads: "+PCounter.rcounter);
            System.out.println("Writes: "+PCounter.wcounter);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }


    // test 8
    // purpose: mark some entries as deleted
    // then run a columnIndex scan
    // should not yield the delete ones
    protected boolean test8()  {
/*
        System.out.println("####################################");
        System.out.println("#### T E S T 8 ####################");
        System.out.println("####################################");

        // columnar file creation logic
        if(numPages == 0)
            return true;
        try {
            String columnarName = "file8";
            int numColumns = 3;
            AttrType[] types = new AttrType[numColumns];
            types[0] = new AttrType(AttrType.attrInteger);
            types[1] = new AttrType(AttrType.attrString);
            types[2] = new AttrType(AttrType.attrString);
            short[] sizes = new short[2];
            sizes[0] = 20;
            sizes[1] = 20;
            System.out.println("Creating columnar " + columnarName);
            String[] attrNames = {"Attr1", "Attr2","Attr3"};
            Columnarfile cf = new Columnarfile(columnarName, numColumns, types, sizes, attrNames);
            //TODO: Should we handle marktupledeleted on columnIndexScan?
            cf.createBitMapIndex(0, new ValueInt<>(4));
            TID tidToMarkedAsDeleted = null;
            TID tid = null;
            System.out.println("Inserting columns..");
            for(int i = 0; i < 50; i++){

                Tuple t = new Tuple();
                t.setHdr((short)3, types, sizes);
                int s = t.size();
                t = new Tuple(s);
                t.setHdr((short)3, types, sizes);

                if(i%2 == 0) {
                    t.setIntFld(1, 4);
                } else {
                    t.setIntFld(1, 5);
                }

                t.setStrFld(2, "A"+i);
                t.setStrFld(3, "B"+i);

                if(i % 3 ==0) {
                    tidToMarkedAsDeleted =  cf.insertTuple(t.getTupleByteArray());
                    cf.markTupleDeleted(tidToMarkedAsDeleted);
                } else {
                    tid =  cf.insertTuple(t.getTupleByteArray());
                }
            }

            // create bitMap file



            BitMapFile bitMap = cf.getBMIndex(cf.getBMName(0, new ValueInt<>(4)));

            String bmName = cf.getBMName(0, new ValueInt<>(4));
            short[] targetedCols = new short[3];

            targetedCols[0] = 0;
            targetedCols[1] = 1;
            targetedCols[2] = 2;

            IndexType indexType = new IndexType(3);


            ColumnarIndexScan columnIndexScan = new ColumnarIndexScan(indexType, columnarName,
                    bmName, null, (short) 1, null, false, targetedCols);


            System.out.println("Starting Index Scan");
            Tuple tuples = columnIndexScan.get_next();


            while (tuples != null){
                System.out.print(tuples.getIntFld(1));
                System.out.print(",");
                System.out.print(tuples.getStrFld(2));
                System.out.print(",");
                System.out.print(tuples.getStrFld(3));
                System.out.println();
                tuples = columnIndexScan.get_next();
            }
            columnIndexScan.close();

            cf.purgeAllDeletedTuples();

            columnIndexScan = new ColumnarIndexScan(indexType, columnarName,
                    bmName, null, (short) 1, null, false, targetedCols);


            System.out.println("Starting Index Scan after purge");
            tuples = columnIndexScan.get_next();


            while (tuples != null){
                System.out.print(tuples.getIntFld(1));
                System.out.print(",");
                System.out.print(tuples.getStrFld(2));
                System.out.print(",");
                System.out.print(tuples.getStrFld(3));
                System.out.println();
                tuples = columnIndexScan.get_next();
            }
            columnIndexScan.close();
            cf.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        System.out.println("Reads: "+  PCounter.rcounter);
        System.out.println("Writes: "+ PCounter.wcounter);
        return true;

    }

    protected String testName() {
        return "Columnar file";
    }
}

public class ColumnarTest {
    public static void runTests() {

        ColumnarDriver cd = new ColumnarDriver();

        cd.runTests();
    }

    public static void main(String[] argvs) {

        try {
            ColumnarTest colTest = new ColumnarTest();
            colTest.runTests();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}*/