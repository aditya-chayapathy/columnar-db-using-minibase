package tests;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.StringKey;
import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.ColumnIndexScan;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

import java.io.IOException;
import java.security.KeyStore.Entry.Attribute;
import java.util.Arrays;
import java.util.Random;


class DeleteQueryingDriver extends TestDriver
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
	public static String deleteType;

	public static String[] inputExpr;
	public static AttrType attval;
	public static CondExpr[] expr;
	public static FldSpec[] Sprojection;
	public static short strsize;
	public static IndexType indtype;
	public static short[]  targetcolnums;
	public static Columnarfile cf;

    public DeleteQueryingDriver() {
        super("DeleteQueryingTest");
    }
    
    

    public DeleteQueryingDriver(String columnDBName2, String columnarFileName2, String expression2,
			int numbuf2, String accessType2, String deleteType) {
		// TODO Auto-generated constructor stub
    	this.columnDBName = columnDBName2;
    	this.columnarFileName = columnarFileName2;
    	
    	this.expression = expression2;
    	this.numbuf = numbuf2;
    	this.accessType = accessType2;
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
			
			int size = cf.getAttributes().length;
			targetcolnums = new short[size];
			for (short i=0;i<size;i++) {
				targetcolnums[i] = i;
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
        
        
        
        try {
        	ColumnarFileScan iscan = new ColumnarFileScan(columnarFileName, cf.getAttributes(), cf.getAttrSizes(),(short)cf.getAttributes().length, targetcolnums.length, Sprojection, expr, targetcolnums);
        	
			boolean status = OK;

	        AttrType[] attrType = cf.getAttributes();
	        short[] attrSize = cf.getAttrSizes();
	        for (int i=0;i<targetcolnums.length; i++) {
	        	attrType[i] = cf.getAttributes()[targetcolnums[i]];
				attrSize[i] = cf.getAttrSizes()[targetcolnums[i]];
	        }
	        // create a tuple of appropriate size
	        Tuple t = new Tuple();
	        t.setHdr((short) targetcolnums.length, attrType, attrSize);
	        
	        int size = t.size();
	        t = new Tuple(size);
	        t.setHdr((short) targetcolnums.length, attrType, attrSize);
	        
	        t = iscan.get_next();
	        while(t!=null) {
	        	
	        	for (int i=0;i<targetcolnums.length; i++) {
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

public class DeleteQueryingTest {
	
	public static String columnDBName;
	public static String columnarFileName;
	public static String targetedColumns;
	public static String expression;
	public static int numbuf;
	public static String accessType; 
	public static String deleteType;
	
    public static void main(String argv[]) {
        boolean queryStatus;
        
        columnDBName = argv[1];
        columnarFileName = argv[2];
        targetedColumns = argv[3];
        expression = argv[4];
        numbuf = Integer.parseInt(argv[5]);
        accessType = argv[6];
        deleteType = argv[7];
        		

        DeleteQueryingDriver queryt = new DeleteQueryingDriver(columnDBName, columnarFileName, expression, numbuf, accessType, deleteType);
        

        queryStatus = queryt.runTests();
        if (queryStatus != true) {
            System.out.println("Error ocurred during index tests");
        } else {
            System.out.println("Index tests completed successfully");
        }
    }
}

