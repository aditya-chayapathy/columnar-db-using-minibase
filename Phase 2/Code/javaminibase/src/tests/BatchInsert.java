package tests;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;

import java.io.*;
import java.util.ArrayList;

import static global.GlobalConst.NUMBUF;

class InsertDriver extends TestDriver {

	private  int numPages = 10000;
	private String dataFile;
	private String dbName;
	private String colFilename;
	private int numCols;
	AttrType[] types;
	short[] sizes;
	ArrayList<String> tuples = new ArrayList<>();
	String[] names;

	//private boolean delete = true;
	public InsertDriver() {
		super("BatchInsert");
	}

	public InsertDriver(String datafileName, String columnDBName, String columnarFileName, int numColumns) {

		super(columnDBName);
		dataFile = datafileName;
		dbName = columnDBName;
		colFilename = columnarFileName;
		numCols = numColumns;
		types = new AttrType[numColumns];
		sizes = new short[numColumns];
		names  = new String[numColumns];
	}

	public boolean runTests() {

		System.out.println("\n" + "Running " + testName() + " tests...." + dbpath+"\n");

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

		System.out.println("Reads: "+  PCounter.rcounter);
		System.out.println("Writes: "+ PCounter.wcounter);
		return _pass;
	}

	protected boolean test1(){
		if(numPages == 0)
			return true;

		FileInputStream fstream;
		try {
			fstream = new FileInputStream(dataFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));


			String strLine;
			//Read First Line
			String attrType = br.readLine();
			String parts[] = attrType.split("\t");
			int i=0;
			for (String s:parts) {
				String temp[] = s.split(":");
				names[i] = temp[0];
				if (temp[1].contains("char")){

					types[i] = new AttrType(AttrType.attrString);
					sizes[i] = Short.parseShort(temp[1].substring(5, temp[1].length()-1));
					i++;
				}
				else {

					types[i] = new AttrType(AttrType.attrInteger);
					sizes[i] = 4;
					i++;
				}
			}


			while ((strLine = br.readLine()) != null)   {
				// Print the content on the console
				tuples.add(strLine);
			}
			//Close the input stream
			br.close();

			int cnt = 1;
			Columnarfile cf = new Columnarfile(colFilename, numCols , types, sizes, names);
			for (String s:tuples) {
				String values[] = s.split("\t");

				Tuple t = new Tuple();
				t.setHdr((short)numCols, types, sizes);
				int size = t.size();

				//System.out.println(size);
				t = new Tuple(size);
				t.setHdr((short)numCols, types, sizes);
				int j =0;
				for (String val:values) {
					switch(types[j].attrType){
						case 0:
							t.setStrFld(j+1, val);
							j++;
							break;
						case 1:
							t.setIntFld(j+1, Integer.parseInt(val));
							j++;
							break;
						default:
							j++;
							break;
					}
				}
				cf.insertTuple(t.getTupleByteArray());
				cnt++;
			}
			System.out.println(cnt +" tuples inserted");

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



		return true;
	}

	protected String testName() {

		return "Batch Insert";
	}
}

public class BatchInsert {

	public static String datafileName;
	public static String columnDBName;
	public static String columnarFileName;
	public static int numColumns;

	public static void runTests() {

		InsertDriver cd = new InsertDriver(datafileName, columnDBName, columnarFileName, numColumns);
		cd.runTests();
	}

	public static void main(String[] argvs) {

		try {
			BatchInsert insertTest = new BatchInsert();

			datafileName = argvs[0];
			columnDBName = argvs[1];
			columnarFileName = argvs[2];
			numColumns = Integer.parseInt(argvs[3]);
			insertTest.runTests();


		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error encountered during buffer manager tests:\n");
			Runtime.getRuntime().exit(1);
		}
	}
}