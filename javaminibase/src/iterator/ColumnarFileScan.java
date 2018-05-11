package iterator;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import global.*;
import heap.*;

import java.io.IOException;

public class ColumnarFileScan extends Iterator{

    private Columnarfile columnarfile;
    private TupleScan scan;
    private Tuple     tuple1;
    private Tuple    Jtuple;
    private CondExpr[]  OutputFilter;
    public FldSpec[] perm_mat;
    Sort deletedTuples;
    private int currDeletePos = -1;
    private AttrType[] targetAttrTypes = null;

    /**
     * constructor
     *
     * @param file_name  columnarfile to be opened
     * @param proj_list  shows what input fields go where in the output tuple
     * @param outFilter  select expressions
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
    public ColumnarFileScan(java.lang.String file_name,
                     FldSpec[] proj_list,
                     short[] targetedCols,
                     CondExpr[] outFilter) throws FileScanException, TupleUtilsException, IOException, InvalidRelation {

        OutputFilter = outFilter;
        perm_mat = proj_list;

        try {
            columnarfile = new Columnarfile(file_name);
            targetAttrTypes = ColumnarScanUtils.getTargetColumnAttributeTypes(columnarfile, targetedCols);
            Jtuple = ColumnarScanUtils.getProjectionTuple(columnarfile, perm_mat, targetedCols);
            scan = columnarfile.openTupleScan(targetedCols);
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(columnarfile.getDeletedFileName());
            if (pid != null) {
                AttrType[] types = new AttrType[1];
                types[0] = new AttrType(AttrType.attrInteger);
                short[] sizes = new	short[0];
                FldSpec[] projlist = new FldSpec[1];
                projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
                FileScan fs = new FileScan(columnarfile.getDeletedFileName(), types, sizes, (short)1, 1, projlist, null);
                deletedTuples = new Sort(types, (short) 1, sizes, fs, 1, new TupleOrder(TupleOrder.Ascending), 4, 10);
            }
        }
        catch(Exception e){
            throw new FileScanException(e, "openScan() failed");
        }
    }

    /**
     *@return shows what input fields go where in the output tuple
     */
    public FldSpec[] show()
    {
        return perm_mat;
    }

    /**
     *@return the result tuple
     *@exception JoinsException some join exception
     *@exception IOException I/O errors
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception PredEvalException exception from PredEval class
     *@exception UnknowAttrType attribute type unknown
     *@exception FieldNumberOutOfBoundException array out of bounds
     *@exception WrongPermat exception for wrong FldSpec argument
     */
    public Tuple get_next()
            throws Exception {

        int position = getNextPosition();

        if (position < 0)
            return null;

        Projection.Project(tuple1, targetAttrTypes, Jtuple, perm_mat, perm_mat.length);
        return Jtuple;
    }

    public boolean delete_next()
            throws Exception {

        int position = getNextPosition();

        if (position < 0)
            return false;

        return columnarfile.markTupleDeleted(position);
    }

    private int getNextPosition()
            throws Exception {
        TID tid = new TID();

        while(true) {
            if((tuple1 =  scan.getNext(tid)) == null) {
                return -1;
            }

            int position = tid.getPosition();
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

            //tuple1.setHdr(in1_len, _in1, s_sizes);
            if (PredEval.Eval(OutputFilter, tuple1, null, targetAttrTypes, null) == true){
                return position;
            }
        }
        return -1;
    }

    /**
     *implement the abstract method close() from super class Iterator
     *to finish cleaning up
     */
    public void close() throws IOException, SortException {

        if (!closeFlag) {
            scan.closetuplescan();
            if(deletedTuples != null)
                deletedTuples.close();
            closeFlag = true;
        }
    }
}

