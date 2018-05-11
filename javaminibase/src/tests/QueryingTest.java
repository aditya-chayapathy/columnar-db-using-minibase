package tests;
/*
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Tuple;
import iterator.ColumnarIndexScan;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;

import java.io.IOException;


class QueryingDriver extends TestDriver
        implements GlobalConst {

    

    private static int LARGE = 1000;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    
    public static String columnDBName;
	public static String columnarFileName;
	public static String targetedColumns;
	public static String expression;
	public static int numbuf;
	public static String accessType;
	public static String[] targetCols;
	public static String[] inputExpr;
	public static AttrType attval;
	public static CondExpr[] expr;
	public static FldSpec[] Sprojection;
	public static short strsize;
	public static IndexType indtype;
	public static short[]  targetcolnums;
	public static Columnarfile cf;

    public QueryingDriver() {
        super("QueryingTest");
    }
    
    

    public QueryingDriver(String columnDBName2, String columnarFileName2, String targetedColumns2, String expression2,
			int numbuf2, String accessType2) {
		// TODO Auto-generated constructor stub
    	this.columnDBName = columnDBName2;
    	this.columnarFileName = columnarFileName2;
    	this.targetedColumns = targetedColumns2;
    	this.expression = expression2;
    	this.numbuf = numbuf2;
    	this.accessType = accessType2;
    	targetCols = targetedColumns.split(",");
    	inputExpr = expression.split(" ");
    	
    	
    	expr = new CondExpr[2];
        expr[0] = new CondExpr();
        
        Sprojection = new FldSpec[1];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        
        switch (inputExpr[1]) {
		case "<":
			expr[0].op = new AttrOperator(AttrOperator.aopLT);
			break;
		case ">" :
			expr[0].op = new AttrOperator(AttrOperator.aopGT);
			break;
		case "=" :
			expr[0].op = new AttrOperator(AttrOperator.aopEQ);
			break;
			
		case "!=" :
			expr[0].op = new AttrOperator(AttrOperator.aopNE);
			break;
		case ">=" :
			expr[0].op = new AttrOperator(AttrOperator.aopGE);
			break;
		case "<=" :
			expr[0].op = new AttrOperator(AttrOperator.aopLE);
			break;
		default:
			break;
		}
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr[0].operand2.string = inputExpr[2];
        expr[0].next = null;
        expr[1] = null;
    	
        
		try {
			cf = new Columnarfile(columnarFileName);
			int val = cf.getAttributePosition(inputExpr[0]);
			attval = cf.getAttributes()[val];
			strsize = cf.getAttrSizes()[val];
			
			int size = targetCols.length;
			targetcolnums = new short[size];
			int i = 0;
	        for (String s:targetCols) {
	        	targetcolnums[i] = (short)cf.getAttributePosition(s);
	        	i++;
	        }
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
    	
    	
	}



	public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(columnDBName, 300, numbuf, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = isUnix() ? "/bin/rm -rf " : "cmd /c del /f ";

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
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        System.out.println("\n" + "..." + testName() + " tests ");
        System.out.println(_pass == OK ? "completely successfully" : "failed");
        System.out.println(".\n\n");

        return _pass;
    }

    protected boolean test1() {
        System.out.println("------------------------ TEST 1 --------------------------");
        
        Iterator iscan = null;
        
        try {
        	switch (accessType) {
			case "Bitmap":
				indtype = new IndexType(IndexType.BitMapIndex);
				iscan = new ColumnarIndexScan(indtype, columnarFileName, inputExpr[0], attval , strsize, expr, false, targetcolnums);
				break;
			case "Btree":
				indtype = new IndexType(IndexType.B_Index);
				iscan = new ColumnarIndexScan(indtype, columnarFileName, inputExpr[0], attval , strsize, expr, false, targetcolnums);
				break;
			case "Filescan":
				iscan = new ColumnarFileScan(columnarFileName, cf.getAttributes(), cf.getAttrSizes(),(short)cf.getAttributes().length, targetcolnums.length, Sprojection, expr, targetcolnums);
				break;
			case "Columnscan":
				iscan = new ColumnarFileScan(columnarFileName, cf.getAttributes(), cf.getAttrSizes(),(short)1, targetcolnums.length, Sprojection, expr, targetcolnums);
				break;
			default:
				break;
			}
        	
			boolean status = OK;

	        AttrType[] attrType = new AttrType[targetCols.length];
	        short[] attrSize = new short[targetCols.length];
	        for (int i=0;i<targetCols.length; i++) {
	        	attrType[i] = cf.getAttributes()[targetcolnums[i]];
				attrSize[i] = cf.getAttrSizes()[targetcolnums[i]];
	        }
	        // create a tuple of appropriate size
	        Tuple t = new Tuple();
	        t.setHdr((short) targetCols.length, attrType, attrSize);
	        
	        int size = t.size();
	        t = new Tuple(size);
	        t.setHdr((short) targetCols.length, attrType, attrSize);
	        
	        t = iscan.get_next();
	        while(t!=null) {
	        	System.out.println(t.getIntFld(1)+","+t.getFloFld(2)+","+t.getStrFld(3));
	        	for (int i=0;i<targetCols.length; i++) {
		        	if(attrType[i].toString().equals("")) {
		        		System.out.print(t.getStrFld(i+1)+ "  ");
		        	}
		        	else {
		        		System.out.print(t.getIntFld(i+1)+"  ");
		        	}
		        }
	        	System.out.println("");
                t = iscan.get_next();
	        } 
        } catch (IndexException | UnknownIndexTypeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
        System.out.println("Reads: "+PCounter.rcounter);
        System.out.println("Writes: "+PCounter.wcounter);
	 
        return true;
    }


    protected boolean test2() {
    	return true;
    }


    protected boolean test3() {
    	return true;
    }

    protected boolean test4() {
        return true;
    }

    protected boolean test5() {
        return true;
    }

    protected boolean test6() {
        return true;
    }

    protected String testName() {
        return "Query";
    }
}

public class QueryingTest {
	
	public static String columnDBName;
	public static String columnarFileName;
	public static String targetedColumns;
	public static String expression;
	public static int numbuf;
	public static String accessType; 

    public static void main(String argv[]) {
        boolean queryStatus;
        
        columnDBName = argv[1];
        columnarFileName = argv[2];
        targetedColumns = argv[3];
        expression = argv[4];
        numbuf = Integer.parseInt(argv[5]);
        accessType = argv[6];
        		

        QueryingDriver queryt = new QueryingDriver(columnDBName, columnarFileName, targetedColumns, expression, numbuf, accessType);
        

        queryStatus = queryt.runTests();
        if (queryStatus != true) {
            System.out.println("Error ocurred during index tests");
        } else {
            System.out.println("Index tests completed successfully");
        }
    }
}
*/
