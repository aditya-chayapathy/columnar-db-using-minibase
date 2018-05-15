package interfaces;

import bufmgr.*;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;

import java.io.*;
import static global.GlobalConst.NUMBUF;

public class BatchInsert {
	private static final int NUM_PAGES = 10000;

	public static void main(String[] args) throws PageUnpinnedException, PagePinnedException, PageNotFoundException, HashOperationException, BufMgrException, IOException {
		// Query Skeleton: DATAFILE COLUMNDB COLUMNARFILE NUMCOLUMNS ISNEWDB
		// Example Query: sampledata.txt testColumnDB columnarTable 4 1
		// Last attribute specifies whether to create a new Db or open an existing one
		String dataFileName = args[0];
		String columnDB = args[1];
		String columnarFile = args[2];
		Integer numColumns = Integer.parseInt(args[3]);
		Integer isNewDb = Integer.parseInt(args[4]);

		String dbpath = InterfaceUtils.dbPath(columnDB);
		int numPages = isNewDb == 1 ? NUM_PAGES : 0;
		SystemDefs sysdef = new SystemDefs(dbpath, numPages, NUMBUF, "Clock");

		runInterface(dataFileName, columnarFile, numColumns);

		SystemDefs.JavabaseBM.flushAllPages();
		SystemDefs.JavabaseDB.closeDB();

		System.out.println("Reads: " + PCounter.rcounter);
		System.out.println("Writes: " + PCounter.wcounter);
	}

	private static void runInterface(String dataFileName, String columnarFile, int numColumns) throws IOException {

		FileInputStream fstream = null;
		BufferedReader br = null;
		try {
			fstream = new FileInputStream(dataFileName);
			br = new BufferedReader(new InputStreamReader(fstream));

			AttrType[] types = new AttrType[numColumns];
			short[] sizes = new short[numColumns];
			String[] names  = new String[numColumns];
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

			Columnarfile cf = new Columnarfile(columnarFile, numColumns , types, sizes, names);

			int cnt = 0;

			while ((strLine = br.readLine()) != null)   {
				String values[] = strLine.split("\t");

				Tuple t = new Tuple();
				t.setHdr((short)numColumns, types, sizes);
				int size = t.size();

				t = new Tuple(size);
				t.setHdr((short)numColumns, types, sizes);
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
			cf.close();
			br.close();
			System.out.println(cnt +" tuples inserted in columnar file : "+columnarFile);
			System.out.println();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			fstream.close();
			br.close();
		}
	}
}