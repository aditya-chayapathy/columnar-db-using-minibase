package iterator;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import global.*;
import heap.*;

import java.io.IOException;

public class ColumnarColumnScan extends Iterator{

    private Columnarfile columnarfile;
    private Scan scan;
    private CondExpr[] OutputFilter;//conditional expression to evaluate the expression
    private CondExpr[] OtherFilter;
    public FldSpec[] perm_mat;
    private AttrType[] _in1 = null;//get the attribute type of the attribute
    private short[] s_sizes = null;//the size of each attribute
    private Heapfile[] targetHeapFiles = null;
    private AttrType[] targetAttrTypes = null;
    private short[] targetShortSizes = null;
    private short[] givenTargetedCols = null;
    private short _tuplesize;
    private Tuple    Jtuple;
    private int _attrsize;
    int _columnNo;
    Sort deletedTuples;
    private int currDeletePos = -1;
	/*file_name specifying the name of the columnar file, columnNo to indicate the column to be scanned, attrType to indicate the type of the attribute, strSize to indicate the size of the particular column, targetedCols to indicate the proje*/
    public ColumnarColumnScan(String file_name,
                              int columnNo,
                              FldSpec[] proj_list,
                              short[] targetedCols,
                              CondExpr[] outFilter,
                              CondExpr[] otherFilter) throws FileScanException, TupleUtilsException, IOException, InvalidRelation {

        givenTargetedCols = targetedCols;
        OutputFilter = outFilter;
        OtherFilter = otherFilter;
        perm_mat = proj_list;
        _columnNo = columnNo;
        try {
            columnarfile = new Columnarfile(file_name);
            targetHeapFiles = ColumnarScanUtils.getTargetHeapFiles(columnarfile, targetedCols);
            targetAttrTypes = ColumnarScanUtils.getTargetColumnAttributeTypes(columnarfile, targetedCols);
            targetShortSizes = ColumnarScanUtils.getTargetColumnStringSizes(columnarfile, targetedCols);
            Jtuple = ColumnarScanUtils.getProjectionTuple(columnarfile, perm_mat, targetedCols);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            scan = columnarfile.openColumnScan(columnNo);
            _in1 = new AttrType[1];
            _in1[0] = columnarfile.getAttrtypeforcolumn(columnNo);
            s_sizes = new	short[1];
            s_sizes[0] = columnarfile.getAttrsizeforcolumn(columnNo);
            _attrsize = columnarfile.getAttrsizeforcolumn(columnNo);
            Tuple t = new Tuple();
            t.setHdr((short)1, _in1, s_sizes);
            _tuplesize = t.size();
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(columnarfile.getDeletedFileName());
            if (pid != null) {

                FldSpec[] projlist = new FldSpec[1];
                projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
                FileScan fs = new FileScan(columnarfile.getDeletedFileName(), _in1, s_sizes, (short)1, 1, projlist, null);
                deletedTuples = new Sort(_in1, (short) 1, s_sizes, fs, 1, new TupleOrder(TupleOrder.Ascending), 4, 10);
            }
        } catch (Exception e) {
            throw new FileScanException(e, "openScan() failed");
        }

    }

    public FldSpec[] show() {
        return perm_mat;
    }

    /**
     * @return the result tuple
     * @throws JoinsException                 some join exception
     * @throws IOException                    I/O errors
     * @throws InvalidTupleSizeException      invalid tuple size
     * @throws InvalidTypeException           tuple type not valid
     * @throws PageNotReadException           exception from lower layer
     * @throws PredEvalException              exception from PredEval class
     * @throws UnknowAttrType                 attribute type unknown
     * @throws FieldNumberOutOfBoundException array out of bounds
     * @throws WrongPermat                    exception for wrong FldSpec argument
     */
    public Tuple get_next()
            throws Exception {

        while (true) {
            int position = getNextPosition();
            if (position < 0)
                return null;
            //tuple1.setHdr(in1_len, _in1, s_sizes);
            Tuple tTuple = null;
            try {
                tTuple = new Tuple();
                tTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
                tTuple = new Tuple(tTuple.size());
                tTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

            for (int i = 0; i < targetHeapFiles.length; i++) {
                Tuple record = targetHeapFiles[i].getRecord(position);
                switch (targetAttrTypes[i].attrType) {
                    case AttrType.attrInteger:
                        // Assumed that col heap page will have only one entry
                        tTuple.setIntFld(i + 1, Convert.getIntValue(0, record.getTupleByteArray()));
                        break;
                    case AttrType.attrString:
                        tTuple.setStrFld(i + 1, Convert.getStrValue(0, record.getTupleByteArray(), targetShortSizes[i] + 2));
                        break;
                    default:
                        throw new Exception("Attribute indexAttrType not supported");
                }
            }

            if (PredEval.Eval(OtherFilter, tTuple, null, targetAttrTypes, null) == true) {
                Projection.Project(tTuple, targetAttrTypes, Jtuple, perm_mat, perm_mat.length);
                return Jtuple;
            }
        }
    }

    public boolean delete_next()
            throws Exception{

        int position = getNextPosition();
        if (position < 0)
            return false;
        return columnarfile.markTupleDeleted(position);
    }

    private int getNextPosition()throws Exception {
        RID rid = new RID();

        Tuple t = null;
        while (true) {
            if ((t = scan.getNext(rid)) == null) {
                return -1;
            }

            Tuple tuple1= new Tuple(_tuplesize);
            tuple1.setHdr((short)1, _in1, s_sizes);
            byte[] data = tuple1.getTupleByteArray();
            System.arraycopy(t.getTupleByteArray(), 0, data, 6, _attrsize);
            t.tupleInit(data, 0, data.length);
            t.setHeaderMetaData();
            if (PredEval.Eval(OutputFilter, t, null, _in1, null) == true) {
                int position = columnarfile.getColumn(_columnNo).positionOfRecord(rid);
                if(deletedTuples != null && position > currDeletePos){
                    while (true){
                        Tuple dtuple = deletedTuples.get_next();
                        if(dtuple == null)
                            break;
                        currDeletePos = dtuple.getIntFld(1);
                        if(currDeletePos >= position)
                            break;
                    }
                }
                if(position == currDeletePos){
                    Tuple dtuple = deletedTuples.get_next();
                    if(dtuple == null)
                        break;
                    currDeletePos = dtuple.getIntFld(1);
                    continue;
                }
                return position;
            }
        }
        return -1;
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     */
    public void close() throws IOException, SortException {

        if (!closeFlag) {
            scan.closescan();
            if(deletedTuples != null)
                deletedTuples.close();
            closeFlag = true;
        }
    }


}
