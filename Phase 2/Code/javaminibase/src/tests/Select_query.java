package tests;

import bitmap.BitMapFile;
import btree.BTreeFile;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Tuple;
import index.ColumnIndexScan;
import iterator.*;

import java.io.IOException;
class ColumnarDriver2 extends TestDriver {

    String DBName;
    String Colfilename;
    String Projection;
    String expression;
    int bufspace;
    String Accesstype;
    int cnt = 0;

    //private boolean delete = true;
    public ColumnarDriver2(String dBName2, String colfilename2, String projection2, String expression2, int bufspace2, String accesstype2) {
        super(dBName2);
        DBName = dBName2;
        Colfilename = colfilename2;
        Projection = projection2;
        expression = expression2;
        bufspace = bufspace2;
        Accesstype = accesstype2;
    }

    public ColumnarDriver2() {


    }

    boolean isInteger(String s) {
        //assumption give only numbers
        try {
            int chumma = Integer.parseInt(s);

        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public boolean runTests() {

        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufspace, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = isUnix() ? "/bin/rm -rf " : "cmd /c del /f ";
        boolean _pass = true;

        if (Accesstype.equals("FILESCAN")) {
            _pass = test1();
        } else if (Accesstype.equals("COLUMNSCAN")) {
            _pass = test2();
        } else if (Accesstype.equals("BTREE")) {
            _pass = test3(0);
        } else if (Accesstype.equals("BITMAP")) {
            _pass = test3(1);
        }
        System.out.println(cnt +" tuples selected");
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        } catch (Exception e) {
            System.err.println("error: " + e);
        }
        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");
        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
        return _pass;

    }

    protected boolean test3(int id) {
        IndexType it;
        String relName = Colfilename;
        StringBuilder sb = new StringBuilder();
        //BM.ColumnarFileName.0.value
        //BT.ColumnarFileName.0.value
        Columnarfile cf;
        try {
            cf = new Columnarfile(Colfilename);

            String[] temp=Projection.split(",");
            String[] expression1=expression.split(" ");
            expression1[0]=expression1[0].replace("{","");
            expression1[2]=expression1[2].replace("}", "");
            int columnNo=cf.getAttributePosition(expression1[0]);
            String indName = "";
            if(id==0){
                sb.append("BT.");
                sb.append(Colfilename);
                sb.append(".");
                sb.append(String.valueOf(columnNo));
                indName = sb.toString();
                it = new IndexType(1);
            }
            else{
                sb.append("BM.");
                sb.append(Colfilename);
                sb.append(".");
                sb.append(String.valueOf(columnNo));
                sb.append(".");
                sb.append(expression1[2]);
                indName = sb.toString();
                it = new IndexType(3);
            }


            if (id == 1) {
                try {
                    BitMapFile bf = cf.getBMIndex(indName);
                    if (bf == null) {
                        throw new Exception("No BMfile");
                    } else {
                        try {
                            AttrType indexAttrType = cf.getAttrtypeforcolumn(columnNo);

                            short[] targetedCols = new short[temp.length];
                          //  AttrType[] opattr=new AttrType[temp.length];
                            boolean indexOnly;
                            if (temp.length == 1) {
                                if (temp[0].equals(expression1[0]))
                                    indexOnly = true;
                                else
                                    indexOnly = false;
                            } else {
                                indexOnly = false;
                            }
                            int index = 0;
                            for (String i : temp) {
                                targetedCols[index] = (short) cf.getAttributePosition(i);
                        //        opattr[index]=new AttrType(cf.getAttrsizeforcolumn(targetedCols[index]));
                                index++;
                            }
                            CondExpr[] expr;
                            if(expression1.length<2){
                                expr=new CondExpr[1];
                                expr[0]=null;
                            }
                            else{
                                expr = new CondExpr[2];
                                expr[0] = new CondExpr();
                                expr[0].next = null;
                                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(expression1[0]) + 1);
                                expr[1] = null;
                                //System.out.println("here");

                                if (expression1[1].equals("=")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
                                } else if (expression1[1].equals(">")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopGT);
                                } else if (expression1[1].equals("<")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopLT);
                                } else if (expression1[1].equals("!=")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopNE);
                                } else if (expression1[1].equals("<=")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopLE);
                                } else if (expression1[1].equals(">=")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopGE);
                                }
                                if (isInteger(expression1[2])) {

                                    expr[0].type2 = new AttrType(AttrType.attrInteger);
                                    expr[0].operand2.integer = Integer.parseInt(expression1[2]);
                                } else {
                                    expr[0].type2 = new AttrType(AttrType.attrString);
                                    expr[0].operand2.string = expression1[2];
                                }
                            }
                            ColumnIndexScan cis = new ColumnIndexScan(it, relName, indName, indexAttrType, cf.getAttrsizeforcolumn(columnNo), expr, indexOnly, targetedCols);
                            boolean done = false;
                            AttrType[] atype2 = new AttrType[temp.length];
                            for (int i = 0; i < temp.length; i++) {
                                atype2[i] = cf.getAttrtypeforcolumn(targetedCols[i]);
                            }
                            int count = 0;
                            while (!done) {
                                Tuple result = cis.get_next();
                                if (result == null) {
                                    done = true;
                                    break;
                                } else {
                                    cnt++;
                                    result.print(atype2);
                                }
                            }
                            cis.close();
                            cf.close();

                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                } catch (HFException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (HFBufMgrException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (HFDiskMgrException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                try {
                    //cf.createAllBitMapIndexForColumn((short)cf.getAttributePosition(expression1[0]));
                    //BM.printBitMap(cf.getBTIndex(cf.getBMName(cf.getAttributePosition(expression1[0]), expression1[2])).getHeaderPage());
                    BTreeFile bf = cf.getBTIndex(indName);
                    if (bf == null) {
                        throw new Exception("No BTfile");
                    } else {
                        try {
                            AttrType indexAttrType = cf.getAttrtypeforcolumn(columnNo);
                            short[] targetedCols = new short[temp.length];
                            boolean indexOnly;
                            if (temp.length == 1) {
                                if (temp[0].equals(expression1[0]))
                                    indexOnly = true;
                                else
                                    indexOnly = false;
                            } else {
                                indexOnly = false;
                            }
                            int index = 0;

                            for (String i : temp) {
                                targetedCols[index++] = (short) cf.getAttributePosition(i);
                            }
                            CondExpr[] expr;
                            if(expression1.length<2){
                                expr=new CondExpr[1];
                                expr[0]=null;
                            }
                            else{
                                expr= new CondExpr[2];
                                expr[0] = new CondExpr();
                                expr[0].next = null;
                                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(expression1[0]) + 1);
                                expr[1] = null;

                                if (expression1[1].equals("=")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
                                } else if (expression1[1].equals(">")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopGT);
                                } else if (expression1[1].equals("<")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopLT);
                                } else if (expression1[1].equals("!=")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopNE);
                                } else if (expression1[1].equals("<=")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopLE);
                                } else if (expression1[1].equals(">=")) {
                                    expr[0].op = new AttrOperator(AttrOperator.aopGE);
                                }
                                if (isInteger(expression1[2])) {

                                    expr[0].type2 = new AttrType(AttrType.attrInteger);
                                    expr[0].operand2.integer = Integer.parseInt(expression1[2]);
                                } else {
                                    expr[0].type2 = new AttrType(AttrType.attrString);
                                    expr[0].operand2.string = expression1[2];
                                }
                            }
                            ColumnIndexScan cis = new ColumnIndexScan(it, relName, indName, indexAttrType, cf.getAttrsizeforcolumn(columnNo), expr, indexOnly, targetedCols);
                            boolean done = false;
                            AttrType[] atype2 = new AttrType[temp.length];
                            for (int i = 0; i < temp.length; i++) {
                                atype2[i] = cf.getAttrtypeforcolumn(targetedCols[i]);
                            }
                            while (!done) {
                                Tuple result = cis.get_next();
                                if (result == null) {
                                    done = true;
                                    break;
                                } else {
                                    result.print(atype2);
                                }
                            }
                            cis.close();
                            cf.close();
                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                } catch (HFException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (HFBufMgrException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (HFDiskMgrException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }


        return true;
    }

    protected boolean test4() {
        return true;
    }

    protected boolean test1() {
        try {
            //System.out.println("here");
            Columnarfile cf=new Columnarfile(Colfilename);
            short len_in1=cf.getnumColumns();
            AttrType[] in1=cf.getAttributes();
            short[] s1_sizes=cf.getAttrSizes();
            String[] temp=Projection.split(",");
            int n_out_flds=temp.length;
            AttrType[] opattr=new AttrType[temp.length];
            String[] expression1=expression.split(" ");
            CondExpr[] expr;
            if(expression1.length<=2){
                expr=new CondExpr[1];
                expr[0]=null;
            }
            else{
                expression1[0]=expression1[0].replace("{","");
                expression1[2]=expression1[2].replace("}", "");
                expr=new CondExpr[2];
                expr[0]=new CondExpr();
                expr[0].next = null;
                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(expression1[0])+1);
                expr[1]=null;
                //System.out.println("here");

                if(expression1[1].equals("=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
                }
                else if(expression1[1].equals(">")){
                    expr[0].op = new AttrOperator(AttrOperator.aopGT);
                }
                else if(expression1[1].equals("<")){
                    expr[0].op = new AttrOperator(AttrOperator.aopLT);
                }
                else if(expression1[1].equals("!=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopNE);
                }
                else if(expression1[1].equals("<=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopLE);
                }
                else if(expression1[1].equals(">=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopGE);
                }
                if (isInteger(expression1[2])) {

                    expr[0].type2 = new AttrType(AttrType.attrInteger);
                    expr[0].operand2.integer = Integer.parseInt(expression1[2]);
                }
                else{
                    expr[0].type2 = new AttrType(AttrType.attrString);
                    expr[0].operand2.string = expression1[2];
                }
            }
            //System.out.println("here");
            FldSpec[] projectionlist=new FldSpec[n_out_flds];
            for(int i=0;i<n_out_flds;i++){
                projectionlist[i]=new FldSpec(new RelSpec(RelSpec.outer),cf.getAttributePosition (temp[i])+1);
                try {
                    opattr[i] = new AttrType(cf.getAttrtypeforcolumn(cf.getAttributePosition(temp[i])).attrType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                ColumnarFileScan fc=new ColumnarFileScan(Colfilename, in1,s1_sizes,len_in1,n_out_flds,projectionlist,expr);
                boolean done=false;
                while(!done){
                    try {
                        Tuple result=fc.get_next();
                        if(result!=null){
                            //result.print(opattr);
                            cnt++;
                        }
                        else{
                            done=true;
                            break;
                        }
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                fc.close();
                cf.close();
            } catch (FileScanException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (TupleUtilsException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InvalidRelation e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }

        } catch (HFException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    protected boolean test2() {
        try {
            //assumption no nested conditions
            Columnarfile cf=new Columnarfile(Colfilename);
            String[] expression1=expression.split(" ");
            CondExpr[] expr;
            int columnNo=cf.getAttributePosition((expression1[0]).replace("{", ""));
            try {
                AttrType attrtype = cf.getAttrtypeforcolumn(columnNo);
                String[] temp=Projection.split(",");
                short[] targetedCols=new short[temp.length];
                int index=0;
                for(String i:temp){
                    targetedCols[index++]=(short) cf.getAttributePosition(i);
                }
                expression1[0]=expression1[0].replace("{","");
                expression1[2]=expression1[2].replace("}", "");

                expr=new CondExpr[2];
                expr[0]=new CondExpr();
                expr[0].next = null;
                //assuming it is always variable to left and it is a character
                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                expr[1] = null;
                if (expression1[1].equals("=")) {
                    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
                }
                else if(expression1[1].equals(">")){
                    expr[0].op = new AttrOperator(AttrOperator.aopGT);
                }
                else if(expression1[1].equals("<")){
                    expr[0].op = new AttrOperator(AttrOperator.aopLT);
                }
                else if(expression1[1].equals("!=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopNE);
                }
                else if(expression1[1].equals("<=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopLE);
                }
                else if(expression1[1].equals(">=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopGE);
                }
                if (isInteger(expression1[2])) {
                    expr[0].type2 = new AttrType(AttrType.attrInteger);
                    expr[0].operand2.integer = Integer.parseInt(expression1[2]);
                }
                else{
                    expr[0].type2 = new AttrType(AttrType.attrString);
                    expr[0].operand2.string = expression1[2];
                }

                AttrType[] atype2 = new AttrType[temp.length];
                for (int i = 0; i < temp.length; i++) {
                    atype2[i] = cf.getAttrtypeforcolumn(targetedCols[i]);
                }

                ColumnarColumnScan ccs = new ColumnarColumnScan(Colfilename, columnNo, attrtype, cf.getAttrsizeforcolumn(columnNo), targetedCols, expr);
                boolean done = false;
                while (!done) {
                    //RID rid = new RID();
                    Tuple result = ccs.get_next();
                    if (result == null) {
                        done = true;
                        break;
                    }
                    result.print(atype2);
                    cnt++;
                }
                ccs.close();
                cf.close();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (HFException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    protected String testName() {

        return "Select Query";
    }
}
public class Select_query extends TestDriver {
    String DBName;
    String Colfilename;
    String Projection;
    String expression;
    int bufspace;
    String Accesstype;

    public boolean runTests() {
        ColumnarDriver2 cd = new ColumnarDriver2(DBName, Colfilename, Projection, expression, bufspace, Accesstype);
        return cd.runTests();
    }
    Select_query(String a, String b, String c, String d, int inputsplit, String access){
        DBName=a;
        Colfilename = b;
        Projection = c;
        expression =d;
        bufspace = inputsplit;
        Accesstype = access;

    }

    public static void main(String args[]) {
        String sampleinput = args[0];
        String[] inputsplit = sampleinput.split(" ");

        if(inputsplit[4].equals("{}")){
            String temp=inputsplit[4];
            Select_query sq = new Select_query(inputsplit[1], inputsplit[2], inputsplit[3], temp, Integer.parseInt(inputsplit[5]), "FILESCAN");
            sq.runTests();
        }
        else{
            String temp = inputsplit[4].replace("{", "") + " " + inputsplit[5] + " " + inputsplit[6].replace("}", "");
            Select_query sq = new Select_query(inputsplit[1], inputsplit[2], inputsplit[3], temp, Integer.parseInt(inputsplit[7]), inputsplit[8]);
            sq.runTests();
        }
    }

}


