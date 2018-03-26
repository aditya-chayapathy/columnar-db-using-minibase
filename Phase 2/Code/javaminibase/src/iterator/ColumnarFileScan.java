package iterator;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import global.AttrType;
import global.PageId;
import global.SystemDefs;
import global.TupleOrder;
import heap.*;

import java.io.IOException;

public class ColumnarFileScan extends Iterator {

    private AttrType[] _in1;
    private short in1_len;
    private short[] s_sizes;
    private Columnarfile f;
    private TupleScan scan;
    private Tuple     tuple1;
    private Tuple    Jtuple;
    private int        t1_size;
    private int nOutFlds;
    private CondExpr[]  OutputFilter;
    public FldSpec[] perm_mat;
    Sort deletedTuples;
    private int currDeletePos = -1;

    public ColumnarFileScan(java.lang.String file_name,
                            AttrType[] in1,
                            short[] s1_sizes,
                            short len_in1,
                            int n_out_flds,
                            FldSpec[] proj_list,
                            CondExpr[] outFilter) throws FileScanException, TupleUtilsException, IOException, InvalidRelation {

        this(file_name,in1,s1_sizes,len_in1,n_out_flds,proj_list,outFilter,null);
    }
    /**
     * constructor
     *
     * @param file_name  columnarfile to be opened
     * @param in1[]      array showing what the attributes of the input fields are.
     * @param s1_sizes[] shows the length of the string fields.
     * @param len_in1    number of attributes in the input tuple
     * @param n_out_flds number of fields in the out tuple
     * @param proj_list  shows what input fields go where in the output tuple
     * @param outFilter  select expressions
     * @throws IOException         some I/O fault
     * @throws FileScanException   exception from this class
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */
    public ColumnarFileScan(java.lang.String file_name,
                     AttrType[] in1,
                     short[] s1_sizes,
                     short len_in1,
                     int n_out_flds,
                     FldSpec[] proj_list,
                     CondExpr[] outFilter,
                     short[] in_cols) throws FileScanException, TupleUtilsException, IOException, InvalidRelation {

        _in1 = in1;
        in1_len = len_in1;
        s_sizes = s1_sizes;

        Jtuple =  new Tuple();
        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[]    ts_size;
        ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);

        OutputFilter = outFilter;
        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        tuple1 =  new Tuple();

        try {
            tuple1.setHdr(in1_len, _in1, s1_sizes);
        }catch (Exception e){
            throw new FileScanException(e, "setHdr() failed");
        }
        t1_size = tuple1.size();

        try {
            f = new Columnarfile(file_name);

        }
        catch(Exception e) {
            throw new FileScanException(e, "Create new columnarfile failed");
        }

        try {
            scan = in_cols == null? f.openTupleScan() : f.openTupleScan(in_cols);
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(f.getDeletedFileName());
            if (pid != null) {
                AttrType[] types = new AttrType[1];
                types[0] = new AttrType(AttrType.attrInteger);
                short[] sizes = new	short[0];
                FldSpec[] projlist = new FldSpec[1];
                projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
                FileScan fs = new FileScan(f.getDeletedFileName(), types, sizes, (short)1, 1, projlist, null);
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

        Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);
        return Jtuple;
    }

    public boolean delete_next()
            throws Exception {

        int position = getNextPosition();

        if (position < 0)
            return false;

        return f.markTupleDeleted(position);
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
                deletedTuples.get_next();
                continue;
            }

            //tuple1.setHdr(in1_len, _in1, s_sizes);
            if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true){
                return position;
            }
        }
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

