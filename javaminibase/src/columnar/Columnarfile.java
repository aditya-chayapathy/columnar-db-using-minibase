package columnar;

import bitmap.BitMapFile;
import btree.*;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.*;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class Columnarfile {
    short numColumns;
    // field to store all the types of the columns
    AttrType[] atype = null;
    // Stores sizes of the Attributes
    short[] attrsizes;

    //Best way handle +2 bytes for strings instead of multiple ifs
    short[] asize;
    // The column files for the c-store
    private Heapfile[] hf = null;
    String fname = null;
    //int tupleCnt = 0;
    Tuple hdr = null;
    RID hdrRid = null;
    // Maps Attributes to position
    HashMap<String, Integer> columnMap;
    // Map to store the BTree indexes
    HashMap<String, BTreeFile> BTMap;
    // Map to store the BitMap indexes
    HashMap<String, BitMapFile> BMMap;

    //for fetching the file

    /**
     *  Opens existing columnar
     *
     * @param name of the columarfile
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws IOException
     */
    public Columnarfile(java.lang.String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile f = null;
        Scan scan = null;
        RID rid = null;
        fname = name;
        columnMap = new HashMap<>();
        try {
            // get the columnar header page
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name + ".hdr");
            if (pid == null) {
                throw new Exception("Columnar with the name: " + name + ".hdr doesn't exists");
            }

            f = new Heapfile(name + ".hdr");

            //Header tuple is organized this way
            //NumColumns, AttrType1, AttrSize1, AttrName1, AttrType2, AttrSize2, AttrName3...

            scan = f.openScan();
            hdrRid = new RID();
            Tuple hdr = scan.getNext(hdrRid);
            hdr.setHeaderMetaData();
            this.numColumns = (short) hdr.getIntFld(1);
            atype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize = new short[numColumns];
            hf = new Heapfile[numColumns];
            int k = 0;
            for (int i = 0; i < numColumns; i++, k = k + 3) {
                atype[i] = new AttrType(hdr.getIntFld(2 + k));
                attrsizes[i] = (short) hdr.getIntFld(3 + k);
                String colName = hdr.getStrFld(4 + k);
                columnMap.put(colName, i);
                asize[i] = attrsizes[i];
                if (atype[i].attrType == AttrType.attrString)
                    asize[i] += 2;
            }
            BTMap = new HashMap<>();
            BMMap = new HashMap<>();

            // create a idx file to store all column which consists of indexes
            pid = SystemDefs.JavabaseDB.get_file_entry(name + ".idx");
            if (pid != null) {
                f = new Heapfile(name + ".idx");
                scan = f.openScan();
                RID r = new RID();
                Tuple t = scan.getNext(r);
                while (t != null) {
                    t.setHeaderMetaData();
                    int indexType = t.getIntFld(1);
                    if (indexType == 0)
                        BTMap.put(t.getStrFld(2), null);
                    else if (indexType == 1)
                        BMMap.put(t.getStrFld(2), null);
                    t = scan.getNext(r);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a new columnar dile
     *
     * @param name
     * @param numcols
     * @param types
     * @param attrSizes
     * @param colnames
     * @throws IOException
     * @throws InvalidTupleSizeException
     * @throws InvalidTypeException
     * @throws FieldNumberOutOfBoundException
     * @throws SpaceNotAvailableException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws InvalidSlotNumberException
     * @throws HFDiskMgrException
     */
    public Columnarfile(java.lang.String name, int numcols, AttrType[] types, short[] attrSizes, String[] colnames) throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        RID rid1 = new RID();
        boolean status = true;
        Heapfile hdrFile = null;
        columnMap = new HashMap<>();
        try {
            hf = new Heapfile[numcols];
            //hf[0] for header file by default
            hdrFile = new Heapfile(name + ".hdr");

        } catch (Exception e) {
            status = false;
            e.printStackTrace();
        }
        if (status == true) {
            numColumns = (short) (numcols);
            this.fname = name;
            atype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize = new short[numColumns];
            int k = 0;
            for (int i = 0; i < numcols; i++) {
                atype[i] = new AttrType(types[i].attrType);
                switch (types[i].attrType) {
                    case 0:
                        asize[i] = attrsizes[i] = attrSizes[k];
                        asize[i] += 2;
                        k++;
                        break;
                    case 1:
                    case 2:
                        asize[i] = attrsizes[i] = 4;
                        break;
                    case 3:
                        asize[i] = attrsizes[i] = 1;
                        break;
                    case 4:
                        attrsizes[i] = 0;
                        break;
                }
            }

            AttrType[] htypes = new AttrType[2 + (numcols * 3)];
            htypes[0] = new AttrType(AttrType.attrInteger);
            for (int i = 1; i < htypes.length - 1; i = i + 3) {
                htypes[i] = new AttrType(AttrType.attrInteger);
                htypes[i + 1] = new AttrType(AttrType.attrInteger);
                htypes[i + 2] = new AttrType(AttrType.attrString);
            }
            htypes[htypes.length - 1] = new AttrType(AttrType.attrInteger);
            short[] hsizes = new short[numcols];
            for (int i = 0; i < numcols; i++) {
                hsizes[i] = 20; //column name can't be more than 20 chars
            }
            hdr = new Tuple();
            hdr.setHdr((short) htypes.length, htypes, hsizes);
            int size = hdr.size();

            hdr = new Tuple(size);
            hdr.setHdr((short) htypes.length, htypes, hsizes);
            hdr.setIntFld(1, numcols);
            int j = 0;
            for (int i = 0; i < numcols; i++, j = j + 3) {
                hdr.setIntFld(2 + j, atype[i].attrType);
                hdr.setIntFld(3 + j, attrsizes[i]);
                hdr.setStrFld(4 + j, colnames[i]);
                columnMap.put(colnames[i], i);
            }
            hdrRid = hdrFile.insertRecord(hdr.returnTupleByteArray());
            BTMap = new HashMap<>();
            BMMap = new HashMap<>();
        }
    }

    /**
     *
     * @throws InvalidSlotNumberException
     * @throws FileAlreadyDeletedException
     * @throws InvalidTupleSizeException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws IOException
     * @throws HFException
     */
    public void deleteColumnarFile() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException, HFException {
        for (int i = 0; i < numColumns; i++) {
            hf[i].deleteFile();
        }
        Heapfile hdr = new Heapfile(fname + "hdr");
        hdr.deleteFile();
        Heapfile idx = new Heapfile(fname + "idx");
        idx.deleteFile();
        hf = null;
        atype = null;
        fname = null;
        numColumns = 0;
    }

    //Assumption: tupleptr contains header information.

    /**
     *
     * @param tuplePtr - insert the tuple and return the TID for the user
     * @return
     * @throws Exception
     */
    public TID insertTuple(byte[] tuplePtr) throws Exception {

        int offset = getOffset();
        RID[] rids = new RID[numColumns];
        int position = 0;
        for (int i = 0; i < numColumns; i++) {

            byte[] data = new byte[asize[i]];
            System.arraycopy(tuplePtr, offset, data, 0, asize[i]);
            rids[i] = getColumn(i).insertRecord(data);
            offset += asize[i];

            // update the indexes accordingly
            String btIndexname = getBTName(i);
            String bmIndexname = getBMName(i, ValueFactory.getValueClass(data, atype[i], asize[i]));
            if (BTMap != null && BTMap.containsKey(btIndexname)) {
                position = getColumn(i).positionOfRecord(rids[i]);
                getBTIndex(btIndexname).insert(KeyFactory.getKeyClass(data, atype[i], asize[i]), position);
            }
            if (BMMap != null && BMMap.containsKey(bmIndexname)) {
                position = getColumn(i).positionOfRecord(rids[i]);
                getBMIndex(bmIndexname).insert(position);
            }
            if(i+1 == numColumns){
                position = getColumn(1).positionOfRecord(rids[1]);
            }
        }
        TID tid = new TID(numColumns, position, rids);
        return tid;
    }

    /**
     * Returns a Tuple for a given TID
     *
     * @param tidarg
     * @return
     * @throws Exception
     */
    public Tuple getTuple(TID tidarg) throws Exception {

        Tuple result = new Tuple(getTupleSize());
        result.setHdr(numColumns, atype, getStrSize());
        byte[] data = result.getTupleByteArray();
        int offset = getOffset();
        for (int i = 0; i < numColumns; i++) {
            Tuple t = getColumn(i).getRecord(tidarg.recordIDs[i]);
            System.arraycopy(t.getTupleByteArray(), 0, data, offset, asize[i]);
            offset += asize[i];
        }

        result.tupleInit(data, 0, data.length);

        return result;
    }

    /**
     * get the value for tidarg and respective column
     *
     * @param tidarg
     * @param column
     * @return
     * @throws Exception
     */
    public ValueClass getValue(TID tidarg, int column) throws Exception {

        Tuple t = getColumn(column).getRecord(tidarg.recordIDs[column]);
        return ValueFactory.getValueClass(t.getTupleByteArray(), atype[column], asize[column]);
    }

    /**
     *
     * @return
     * @throws HFDiskMgrException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws IOException
     * @throws InvalidTupleSizeException
     * @throws InvalidSlotNumberException
     */
    public int getTupleCnt() throws HFDiskMgrException, HFException,
            HFBufMgrException, IOException, InvalidTupleSizeException, InvalidSlotNumberException {
        return getColumn(0).getRecCnt();
    }

    /**
     * opens the tuple scan on all the columns by open all the heap files
     *
     * @return
     * @throws Exception
     */
    public TupleScan openTupleScan() throws Exception {

        TupleScan result = new TupleScan(this);
        return result;
    }

    /**
     * opens tuple scan on the given columns
     *
     * @param columns
     * @return
     * @throws Exception
     */
    public TupleScan openTupleScan(short[] columns) throws Exception {
        TupleScan result = new TupleScan(this, columns);
        return result;
    }

    /**
     * opens tuple scan for the given column
     * @param columnNo
     * @return
     * @throws Exception
     */
    public Scan openColumnScan(int columnNo) throws Exception {
        Scan scanobj = null;
        if (columnNo < hf.length) {
            scanobj = new Scan(getColumn(columnNo));
        } else {

            throw new Exception("Invalid Column number");
        }

        return scanobj;
    }

    /**
     * updates the tuple with given newTuple for the TID argument
     *
     * @param tidarg
     * @param newtuple
     * @return
     */
    public boolean updateTuple(TID tidarg, Tuple newtuple) {
        try {

            int offset = getOffset();
            byte[] tuplePtr = newtuple.getTupleByteArray();
            for (int i = 0; i < numColumns; i++) {

                byte[] data = new byte[asize[i]];
                System.arraycopy(tuplePtr, offset, data, 0, asize[i]);
                Tuple t = new Tuple(asize[i]);
                t.tupleInit(data, 0, data.length);
                getColumn(i).updateRecord(tidarg.recordIDs[i], t);
                offset += asize[i];
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     *
     * @param tidarg
     * @param newtuple
     * @param column
     * @return
     */
    public boolean updateColumnofTuple(TID tidarg, Tuple newtuple, int column) {
        try {
            int offset = getOffset(column);
            byte[] tuplePtr = newtuple.getTupleByteArray();
            Tuple t = new Tuple(asize[column]);
            byte[] data = t.getTupleByteArray();
            System.arraycopy(tuplePtr, offset, data, 0, asize[column]);
            t.tupleInit(data, 0, data.length);
            getColumn(column).updateRecord(tidarg.recordIDs[column], t);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public boolean createBTreeIndex(int columnNo) throws Exception {
        String indexName = getBTName(columnNo);

        int keyType = atype[columnNo].attrType;
        int keySize = asize[columnNo];
        int deleteFashion = 0;
        BTreeFile bTreeFile = new BTreeFile(indexName, keyType, keySize, deleteFashion);
        Scan columnScan = openColumnScan(columnNo);
        RID rid = new RID();
        Tuple tuple;
        while (true) {
            tuple = columnScan.getNext(rid);
            if (tuple == null) {
                break;
            }
            int position = getColumn(columnNo).positionOfRecord(rid);
            bTreeFile.insert(KeyFactory.getKeyClass(tuple.getTupleByteArray(), atype[columnNo], asize[columnNo]), position);
        }
        columnScan.closescan();
        addIndexToColumnar(0, indexName);
        return true;
    }

    /**
     *
     * @param columnNo
     * @param value
     * @return
     * @throws Exception
     */
    public boolean createBitMapIndex(int columnNo, ValueClass value) throws Exception {

        short[] targetedCols = new short[1];
        targetedCols[0] = (short) columnNo;

        FldSpec[] projection = new FldSpec[1];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        ColumnarColumnScan columnScan = new ColumnarColumnScan(getColumnarFileName(), columnNo,
                projection,
                targetedCols,
                null, null);

        String indexName = getBMName(columnNo, value);
        BitMapFile bitMapFile = new BitMapFile(indexName, this, columnNo, value);
        Tuple tuple;
        int position = 0;
        while (true) {
            tuple = columnScan.get_next();
            if (tuple == null) {
                break;
            }
            ValueClass valueClass = ValueFactory.getValueClass(tuple.getTupleByteArray(), atype[columnNo], asize[columnNo]);
            if (valueClass.toString().equals(value.toString())) {
                bitMapFile.insert(position);
            } else {
                bitMapFile.delete(position);
            }
            position++;
        }
        columnScan.close();
        bitMapFile.close();

        addIndexToColumnar(1, indexName);

        return true;
    }

    /**
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public boolean createAllBitMapIndexForColumn(int columnNo) throws Exception {
        short[] targetedCols = new short[1];
        targetedCols[0] = (short) columnNo;

        FldSpec[] projection = new FldSpec[1];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        ColumnarColumnScan columnScan = new ColumnarColumnScan(getColumnarFileName(), columnNo,
                projection,
                targetedCols,
                null, null);

        RID rid = new RID();
        Tuple tuple;
        int position = 0;
        Set<BitMapFile> bitMapFiles = new HashSet<>();
        while (true) {
            tuple = columnScan.get_next();
            if (tuple == null) {
                break;
            }

            ValueClass valueClass;
            if (atype[columnNo].attrType == AttrType.attrInteger) {
                valueClass = new ValueInt(tuple.getIntFld(1));
            } else {
                valueClass = new ValueString(tuple.getStrFld(1));
            }

            BitMapFile bitMapFile;
            String bitMapFileName = getBMName(columnNo, valueClass);
            if (!BMMap.containsKey(bitMapFileName)) {
                bitMapFile = new BitMapFile(bitMapFileName, this, columnNo, valueClass);
                addIndexToColumnar(1, bitMapFileName);
                BMMap.put(bitMapFileName, bitMapFile);
            } else {
                bitMapFile = getBMIndex(bitMapFileName);
            }
            bitMapFiles.add(bitMapFile);

            for (BitMapFile existingBitMapFile : bitMapFiles) {
                if (existingBitMapFile.getHeaderPage().getValue().equals(valueClass.toString())) {
                    existingBitMapFile.insert(position);
                } else {
                    existingBitMapFile.delete(position);
                }
            }

            position++;
        }
        columnScan.close();
        for (BitMapFile bitMapFile : bitMapFiles) {
            bitMapFile.close();
        }

        return true;
    }

    /**
     * Marks all records at position as deleted. Scan skips over these records
     *
     * @param position
     * @return
     */
    public boolean markTupleDeleted(int position) {
        String name = getDeletedFileName();
        try {
            Heapfile f = new Heapfile(name);
            Integer pos = position;
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrInteger);
            short[] sizes = new short[0];
            Tuple t = new Tuple(10);
            t.setHdr((short) 1, types, sizes);
            t.setIntFld(1, pos);
            f.insertRecord(t.getTupleByteArray());

            for (int i = 0; i < numColumns; i++) {
                Tuple tuple = getColumn(i).getRecord(position);
                ValueClass valueClass;
                KeyClass keyClass;
                valueClass = ValueFactory.getValueClass(tuple.getTupleByteArray(),
                        atype[i],
                        asize[i]);
                keyClass = KeyFactory.getKeyClass(tuple.getTupleByteArray(),
                        atype[i],
                        asize[i]);

                String bTreeFileName = getBTName(i);
                String bitMapFileName = getBMName(i, valueClass);
                if (BTMap.containsKey(bTreeFileName)) {
                    BTreeFile bTreeFile = getBTIndex(bTreeFileName);
                    bTreeFile.Delete(keyClass, position);
                }
                if (BMMap.containsKey(bitMapFileName)) {
                    BitMapFile bitMapFile = getBMIndex(bitMapFileName);
                    bitMapFile.delete(position);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     *
     * @param tidarg
     * @return
     */
    public boolean markTupleDeleted(TID tidarg) {
        return markTupleDeleted(tidarg.position);
    }

    /**
     * Purges all tuples marked for deletion. Removes keys/positions from indexes too
     *
     * @return
     * @throws HFDiskMgrException
     * @throws InvalidTupleSizeException
     * @throws IOException
     * @throws InvalidSlotNumberException
     * @throws FileAlreadyDeletedException
     * @throws HFBufMgrException
     * @throws SortException
     */
    public boolean purgeAllDeletedTuples() throws HFDiskMgrException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException, SortException {

        boolean status = OK;
        Sort deletedTuples = null;
        RID rid;
        Heapfile f = null;
        int pos_marked;
        boolean done = false;
        try {
            f = new Heapfile(getDeletedFileName());
        } catch (Exception e) {
            status = FAIL;
            System.err.println(" Could not open heapfile");
            e.printStackTrace();
        }

        if (status == OK) {
            try {
                AttrType[] types = new AttrType[1];
                types[0] = new AttrType(AttrType.attrInteger);
                short[] sizes = new short[0];
                FldSpec[] projlist = new FldSpec[1];
                projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
                FileScan fs = new FileScan(getDeletedFileName(), types, sizes, (short) 1, 1, projlist, null);
                deletedTuples = new Sort(types, (short) 1, sizes, fs, 1, new TupleOrder(TupleOrder.Descending), 4, 10);

            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {
            int i = 0;
            Tuple tuple;
            while (!done) {
                try {
                    rid = new RID();
                    tuple = deletedTuples.get_next();
                    if (tuple == null) {
                        deletedTuples.close();
                        done = true;
                        break;
                    }
                    pos_marked = Convert.getIntValue(6, tuple.getTupleByteArray());
                    for (int j = 0; j < numColumns; j++) {
                        rid = getColumn(j).recordAtPosition(pos_marked);
                        getColumn(j).deleteRecord(rid);

                        for (String fileName : BMMap.keySet()) {
                            int columnNo = Integer.parseInt(fileName.split("\\.")[2]);
                            if (columnNo == i) {
                                BitMapFile bitMapFile = getBMIndex(fileName);
                                bitMapFile.fullDelete(pos_marked);
                            }
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    if(deletedTuples != null)
                        deletedTuples.close();
                    f.deleteFile();
                    return false;
                }
            }
        }
        f.deleteFile();

        return true;
    }

    /**
     * Write the indexes created on each column to the .idx file
     *
     * @param indexType
     * @param indexName
     * @return
     */
    private boolean addIndexToColumnar(int indexType, String indexName) {

        try {
            AttrType[] itypes = new AttrType[2];
            itypes[0] = new AttrType(AttrType.attrInteger);
            itypes[1] = new AttrType(AttrType.attrString);
            short[] isizes = new short[1];
            isizes[0] = 40; //index name can't be more than 40 chars
            Tuple t = new Tuple();
            t.setHdr((short) 2, itypes, isizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short) 2, itypes, isizes);
            t.setIntFld(1, indexType);
            t.setStrFld(2, indexName);
            Heapfile f = new Heapfile(fname + ".idx");
            f.insertRecord(t.getTupleByteArray());

            if (indexType == 0) {
                BTMap.put(indexName, null);
            } else if (indexType == 1) {
                BMMap.put(indexName, null);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Return the respective column heap file
     *
     * @param columnNo
     * @return
     * @throws IOException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     */
    public Heapfile getColumn(int columnNo) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        if (hf[columnNo] == null)
            hf[columnNo] = new Heapfile(fname + columnNo);
        return hf[columnNo];
    }

    /**
     * return the BTree index for the given indexName
     *
     * @param indexName
     * @return
     * @throws IOException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws ConstructPageException
     * @throws GetFileEntryException
     * @throws PinPageException
     */
    public BTreeFile getBTIndex(String indexName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, ConstructPageException, GetFileEntryException, PinPageException {
        if (!BTMap.containsKey(indexName))
            return null;
        if (BTMap.get(indexName) == null)
            BTMap.put(indexName, new BTreeFile(indexName));

        return BTMap.get(indexName);
    }

    /**
     *  Return bitmap file for the given indexName
     *
     * @param indexName
     * @return
     * @throws Exception
     */
    public BitMapFile getBMIndex(String indexName) throws Exception {
        if (!BMMap.containsKey(indexName))
            return null;
        if (BMMap.get(indexName) == null)
            BMMap.put(indexName, new BitMapFile(indexName));

        return BMMap.get(indexName);
    }

    /**
     * remove all the dangling files for the store
     */
    public void close() {
        if (hf != null) {
            for (int i = 0; i < hf.length; i++)
                hf[i] = null;
        }
        try {
            if (BTMap != null) {
                for (BTreeFile bt : BTMap.values()) {
                    if (bt != null) {
                        bt.close();
                    }
                }
            }
            if (BMMap != null) {
                for (BitMapFile bm : BMMap.values()) {
                    if (bm != null) {
                        bm.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error closing columnar: " + fname);
        }
    }

    public int getTupleSize() {

        int size = getOffset();
        for (int i = 0; i < numColumns; i++) {
            size += asize[i];
        }
        return size;
    }

    public short[] getStrSize() {

        int n = 0;
        for (int i = 0; i < numColumns; i++) {
            if (atype[i].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for (int i = 0; i < numColumns; i++) {
            if (atype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = attrsizes[i];
            }
        }

        return strSize;
    }

    public short[] getStrSize(short[] targetColumns) {

        int n = 0;
        for (int i = 0; i < targetColumns.length; i++) {
            if (atype[targetColumns[i]].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for (int i = 0; i < targetColumns.length; i++) {
            if (atype[targetColumns[i]].attrType == AttrType.attrString) {
                strSize[cnt++] = attrsizes[targetColumns[i]];
            }
        }

        return strSize;
    }

    public int getOffset() {
        return 4 + (numColumns * 2);
    }

    public int getOffset(int column) {
        int offset = 4 + (numColumns * 2);
        for (int i = 0; i < column; i++) {
            offset += asize[i];
        }
        return offset;
    }

    public String getColumnarFileName() {
        return fname;
    }


    public AttrType[] getAttributes() {
        return atype;
    }

    public short[] getAttrSizes() {
        return attrsizes;
    }


    public int getAttributePosition(String name) {
        return columnMap.get(name);
    }

    /**
     * return the BT Name
     *
     * @param columnNo
     * @return
     */
    public String getBTName(int columnNo) {
        return "BT" + "." + fname + "." + columnNo;
    }

    /**
     * return the BitMap file name by following the conventions
     *
     * @param columnNo
     * @param value
     * @return
     */
    public String getBMName(int columnNo, ValueClass value) {
        return "BM" + "." + fname + "." + columnNo + "." + value.toString();
    }

    public String[] getAvailableBM(int columnNo) {
        List<String> bmName = new ArrayList<>();
        String prefix = "BM" + "." + fname + "." + columnNo + ".";
        for(String s : BMMap.keySet()){
            if(s.substring(0,prefix.length()).equals(prefix)){
                bmName.add(s);
            }
        }
        return  bmName.toArray(new String[bmName.size()]);
    }

    public String getDeletedFileName() {
        return fname + ".del";
    }



    public short getnumColumns() {
        return numColumns;
    }

    /**
     * given a column returns the AttrType
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public AttrType getAttrtypeforcolumn(int columnNo) throws Exception {
        if (columnNo < numColumns) {
            return atype[columnNo];
        } else {
            throw new Exception("Invalid Column Number");
        }
    }

    /**
     *  given the column returns the size of AttrString
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public short getAttrsizeforcolumn(int columnNo) throws Exception {
        if (columnNo < numColumns) {
            return attrsizes[columnNo];
        } else {
            throw new Exception("Invalid Column Number");
        }
    }

    //phase 3

    public HashMap<String, BitMapFile> getAllBitMaps() {
        return BMMap;
    }

    public Tuple getTuple(int position) throws
            Exception {

        for(int i=0; i < hf.length; i++) {
            hf[i] = new Heapfile(getColumnarFileName() + i);
        }
        Tuple JTuple = new Tuple();
        // set the header which attribute types of the targeted columns
        JTuple.setHdr((short) hf.length, atype, getStrSize());

        JTuple = new Tuple(JTuple.size());
        JTuple.setHdr((short) hf.length, atype, getStrSize());
        for (int i = 0; i < hf.length; i++) {
            RID rid = hf[i].recordAtPosition(position);
            Tuple record = hf[i].getRecord(rid);
            switch (atype[i].attrType) {
                case AttrType.attrInteger:
                    // Assumed that col heap page will have only one entry
                    JTuple.setIntFld(i + 1,
                            Convert.getIntValue(0, record.getTupleByteArray()));
                    break;
                case AttrType.attrString:
                    JTuple.setStrFld(i + 1,
                            Convert.getStrValue(0, record.getTupleByteArray(), attrsizes[i] + 2));
                    break;
                default:
                    throw new Exception("Attribute indexAttrType not supported");
            }
        }

        return JTuple;
    }

}
