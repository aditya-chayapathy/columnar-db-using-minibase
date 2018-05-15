package columnar;

import global.AttrType;
import global.RID;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;

public class TupleScan {
    private Columnarfile file;
	Scan[] sc;

	private AttrType[] atype = null;
	private short[] asize = null;
	private short[] strSize = null;
	private short numColumns;
	private int toffset;
	private int tuplesize;

    /**
     * Initiates scan on all columns
     *
     * @param f Columnarfile object
     * @throws InvalidTupleSizeException
     * @throws IOException
     */
	public TupleScan(Columnarfile f) throws Exception {
        file = f;
        numColumns = f.numColumns;
        atype = f.atype;
        asize = f.asize;
        strSize = f.getStrSize();
        toffset = f.getOffset();
        tuplesize = f.getTupleSize();
        sc=new Scan[numColumns];
        for(int i=0;i<numColumns;i++){
            sc[i] = f.getColumn(i).openScan();
        }
    }

    /**
     * This constructor truly takes advantage of columnar organization by scanning only
     * required columns.
     *
     * @param f Columnarfile object
     * @param columns array of column numbers that need to be scanned
     * @throws InvalidTupleSizeException
     * @throws IOException
     */
    public TupleScan(Columnarfile f,short[] columns) throws Exception {
        file = f;
        numColumns = (short)columns.length;
        atype = new AttrType[numColumns];
        asize = new short[numColumns];
        sc=new Scan[numColumns];
        short strCnt = 0;
        for(int i=0;i<numColumns;i++){

            short c = columns[i];
            atype[i] = f.atype[c];
            asize[i] = f.asize[c];
            sc[i] = f.getColumn(c).openScan();

            if(atype[i].attrType == AttrType.attrString)
                strCnt++;
        }

        strSize = new short[strCnt];
        toffset = 4 + (numColumns * 2);
        tuplesize = toffset;
        int cnt = 0;
        for(int i = 0; i < numColumns; i++){
            short c = columns[i];
            if(atype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = f.attrsizes[c];
            }
            tuplesize += asize[i];
        }
    }
	public void closetuplescan(){
		for(int i=0;i<sc.length;i++){
			sc[i].closescan();
		}
        file.close();
	}
	public Tuple getNext(TID tid) throws Exception {

        Tuple result = new Tuple(tuplesize);
        result.setHdr(numColumns, atype, strSize);
        byte[] data = result.getTupleByteArray();
        RID[] rids = new RID[sc.length];
        RID rid = new RID();
        int position = 0;
        boolean canContinue;
        int offset = toffset;
        for (int i = 0; i < numColumns; i++) {
            Tuple t = sc[i].getNext(rid);
            if (t == null)
                return null;

            rids[i] = new RID();
            rids[i].copyRid(rid);
            rid = new RID();
            int size = asize[i];
            System.arraycopy(t.getTupleByteArray(), 0, data, offset, size);
            offset += asize[i];
            if(i+1 == numColumns)
                position = sc[i].positionOfRecord(rids[i]);
        }

        tid.numRIDs = sc.length;
        tid.recordIDs = rids;
        tid.setPosition(position);
        result.tupleInit(data, 0, data.length);

        return result;

    }
	public boolean position(TID tidarg){
		RID[] ridstemp=new RID[tidarg.numRIDs];
		for(int i=0;i<tidarg.numRIDs;i++){
			ridstemp[i].copyRid(tidarg.recordIDs[i]);
			try {
				boolean ret=sc[i].position(ridstemp[i]);
				if(ret==false){
					return false;
				}
			} catch (InvalidTupleSizeException e) {

				e.printStackTrace();
			} catch (IOException e) {

				e.printStackTrace();
			}
		}
		return true;
	}

}