package iterator;

import global.*;
import heap.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The Sort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class ColumnarSort extends Iterator implements GlobalConst {
    private static final int ARBIT_RUNS = 10;

    private AttrType[] _in;
    private short n_cols;
    private short[] str_lens;
    private Iterator _am;
    private int _sort_fld;
    private TupleOrder order;
    private int _n_pages;
    private byte[][] bufs;
    private boolean first_time;
    private int Nruns;
    private int max_elems_in_heap;
    private int sortFldLen;
    private int tuple_size;
    Tuple[] tuples;

    private List<RunFile> temp_files;
    private int n_tempfiles;
    private Tuple output_tuple;
    private List<Integer> n_tuples;
    private int n_runs;
    private Tuple op_buf;
    private COBuf o_buf;
    private PageId[] bufs_pids;
    private boolean useBM = true; // flag for whether to use buffer manager
    private int passes = 0;

    /**
     * Class constructor, take information about the tuples, and set up
     * the sorting
     *
     * @param in             array containing attribute types of the relation
     * @param len_in         number of columns in the relation
     * @param str_sizes      array of sizes of string attributes
     * @param am             an iterator for accessing the tuples
     * @param sort_fld       the field number of the field to sort on
     * @param sort_order     the sorting order (ASCENDING, DESCENDING)
     * @param n_pages        amount of memory (in pages) available for sorting
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    public ColumnarSort(AttrType[] in,
                        short len_in,
                        short[] str_sizes,
                        Iterator am,
                        int sort_fld,
                        TupleOrder sort_order,
                        int sort_fld_len,
                        int n_pages
    ) throws IOException, SortException {
        _in = new AttrType[len_in];
        n_cols = len_in;
        int n_strs = 0;

        for (int i = 0; i < len_in; i++) {
            _in[i] = new AttrType(in[i].attrType);
            if (in[i].attrType == AttrType.attrString) {
                n_strs++;
            }
        }

        str_lens = new short[n_strs];

        n_strs = 0;
        for (int i = 0; i < len_in; i++) {
            if (_in[i].attrType == AttrType.attrString) {
                str_lens[n_strs] = str_sizes[n_strs];
                n_strs++;
            }
        }

        Tuple t = new Tuple(); // need Tuple.java
        try {
            t.setHdr(len_in, _in, str_sizes);
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: t.setHdr() failed");
        }
        tuple_size = t.size();

        _am = am;
        _sort_fld = sort_fld;
        order = sort_order;
        _n_pages = n_pages;

        // this may need change, bufs ???  need io_bufs.java
        //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
        bufs_pids = new PageId[_n_pages-1];
        bufs = new byte[_n_pages-1][];

        if (useBM) {
            try {
                get_buffer_pages(_n_pages-1, bufs_pids, bufs);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: BUFmgr error");
            }
        } else {
            for (int k = 0; k < _n_pages; k++) bufs[k] = new byte[MAX_SPACE];
        }

        first_time = true;

        // as a heuristic, we set the number of runs to an arbitrary value
        // of ARBIT_RUNS
        temp_files = new ArrayList<>();
        n_tempfiles = ARBIT_RUNS;
        n_tuples = new ArrayList<>();
        n_runs = ARBIT_RUNS;

        o_buf = new COBuf();

        max_elems_in_heap = 200;
        sortFldLen = sort_fld_len;
    }

    /**
     * Set up for merging the runs.
     * Open an input buffer for each run, and insert the first element (min)
     * from each run into a heap. <code>delete_min() </code> will then get
     * the minimum of all runs.
     *
     * @param tuple_size size (in bytes) of each tuple
     * @throws IOException     from lower layers
     * @throws LowMemException there is not enough memory to
     *                         sort in two passes (a subclass of SortException).
     * @throws SortException   something went wrong in the lower layer.
     * @throws Exception       other exceptions
     */
    private void setup_for_merge(int tuple_size) throws HFBufMgrException, IOException, InvalidTupleSizeException, InvalidTypeException, UnknowAttrType, FieldNumberOutOfBoundException, HFException, HFDiskMgrException, InvalidSlotNumberException, TupleUtilsException, SpaceNotAvailableException, FileAlreadyDeletedException {

        int size = temp_files.size();
        tuples = new Tuple[size];
        passes = 1;
        while (size >= _n_pages) {
            passes++;
            int i = 0;
            int x = 0;
            while (i < size) {
                int j = i;
                for (; j < i + _n_pages - 1; j++) {
                    if (j < size) {
                        tuples[j] = null;
                        temp_files.get(j).initiateScan();
                    }
                }

                RunFile op = new RunFile();
                op.initiateSequentialInsert();
                Tuple t = new Tuple(tuple_size);
                t.setHdr(n_cols, _in, str_lens);


                boolean done = false;
                while (!done) {
                    if (order.tupleOrder == TupleOrder.Ascending)
                        MAX_VAL(t, _in[_sort_fld - 1]);
                    else
                        MIN_VAL(t, _in[_sort_fld - 1]);
                    done = true;
                    int k = -1;
                    j = i;
                    for (; j < i + _n_pages - 1; j++) {
                        if (j < size) {
                            if(tuples[j] == null)
                                tuples[j] = temp_files.get(j).getNext();
                            if (tuples[j] != null) {
                                tuples[j].setHdr(n_cols, _in, str_lens);
                                int ans = TupleUtils.CompareTupleWithTuple(_in[_sort_fld - 1], tuples[j], _sort_fld, t, _sort_fld);
                                if ((order.tupleOrder == TupleOrder.Ascending && ans == -1) ||
                                        (order.tupleOrder == TupleOrder.Descending && ans == 1)) {
                                    t = new Tuple(tuples[j]);
                                    k = j;
                                }
                            }
                        }
                    }
                    if (k > -1) {
                        done = false;
                        op.insertNext(t.returnTupleByteArray());
                        tuples[k] = null;
                        /*int l = i;
                        for (; l < i +_n_pages - 1; l++) {
                            if (l < size && l != k) {
                                temp_files.get(l).setPrev();
                            }
                        }*/
                    }
                }
                for (; i < j; i++) {
                    if (i < size) {
                        temp_files.get(i).finishScan();
                        temp_files.get(i).deleteFile();
                        temp_files.set(i, null);
                    }
                }
                op.finishSequentialInsert();
                temp_files.set(x, op);
                x++;
            }
            size = x;
        }
    }

    /**
     * Generate sorted runs.
     * Using heap sort.
     *
     * @param max_elems   maximum number of elements in heap
     * @param sortFldType attribute type of the sort field
     * @return number of runs generated
     * @throws IOException    from lower layers
     * @throws SortException  something went wrong in the lower layer.
     * @throws JoinsException from <code>Iterator.get_next()</code>
     */
    private int generate_runs(int max_elems, AttrType sortFldType)
            throws IOException,
            SortException,
            UnknowAttrType,
            TupleUtilsException,
            JoinsException,
            Exception {
        temp_files.add(new RunFile());
        temp_files.get(0).initiateSequentialInsert();
        o_buf.init(bufs, _n_pages-1, tuple_size, temp_files.get(0), false);
        op_buf = new Tuple(tuple_size);   // need Tuple.java
        try {
            op_buf.setHdr(n_cols, _in, str_lens);
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
        }
        Tuple tuple;
        pnode cur_node;
        pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, sortFldType, order);
        pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, sortFldType, order);
        pnodeSplayPQ pcurr_Q = Q1;
        pnodeSplayPQ pother_Q = Q2;
        Tuple lastElem = new Tuple(tuple_size);  // need tuple.java
        try {
            lastElem.setHdr(n_cols, _in, str_lens);
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: setHdr() failed");
        }

        int run_num = 0;  // keeps track of the number of runs

        // number of elements in Q
        //    int nelems_Q1 = 0;
        //    int nelems_Q2 = 0;
        int p_elems_curr_Q = 0;
        int p_elems_other_Q = 0;

        int comp_res;

        // set the lastElem to be the minimum value for the sort field
        if (order.tupleOrder == TupleOrder.Ascending) {
            try {
                MIN_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
                throw new SortException(e, "MIN_VAL failed");
            }
        } else {
            try {
                MAX_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
                throw new SortException(e, "MIN_VAL failed");
            }
        }

        // maintain a fixed maximum number of elements in the heap
        while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
            try {
                tuple = _am.get_next();  // according to Iterator.java
            } catch (Exception e) {
                e.printStackTrace();
                throw new SortException(e, "Sort.java: get_next() failed");
            }

            if (tuple == null) {
                break;
            }
            cur_node = new pnode();
            cur_node.tuple = new Tuple(tuple); // tuple copy needed --  Bingjie 4/29/98

            pcurr_Q.enq(cur_node);
            p_elems_curr_Q++;
        }

        // now the queue is full, starting writing to file while keep trying
        // to add new tuples to the queue. The ones that does not fit are put
        // on the other queue temperarily
        while (true) {
            cur_node = pcurr_Q.deq();
            if (cur_node == null) break;
            p_elems_curr_Q--;

            comp_res = TupleUtils.CompareTupleWithValue(sortFldType, cur_node.tuple, _sort_fld, lastElem);  // need tuple_utils.java

            if ((comp_res < 0 && order.tupleOrder == TupleOrder.Ascending) || (comp_res > 0 && order.tupleOrder == TupleOrder.Descending)) {
                // doesn't fit in current run, put into the other queue
                try {
                    pother_Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                }
                p_elems_other_Q++;
            } else {
                // set lastElem to have the value of the current tuple,
                // need tuple_utils.java
                TupleUtils.SetValue(lastElem, cur_node.tuple, _sort_fld, sortFldType);
                // write tuple to output file, need io_bufs.java, type cast???
                //	System.out.println("Putting tuple into run " + (run_num + 1));
                //	cur_node.tuple.print(_in);

                o_buf.Put(cur_node.tuple);
            }

            // check whether the other queue is full
            if (p_elems_other_Q == max_elems) {
                // close current run and start next run
                n_tuples.add((int) o_buf.flush());  // need io_bufs.java
                run_num++;

                try {
                    temp_files.get(run_num-1).finishSequentialInsert();
                    temp_files.add(new RunFile());
                    temp_files.get(run_num).initiateSequentialInsert();
                } catch (Exception e) {
                    throw new SortException(e, "Sort.java: create Heapfile failed");
                }

                // need io_bufs.java
                o_buf.init(bufs, _n_pages-1, tuple_size, temp_files.get(run_num), false);

                // set the last Elem to be the minimum value for the sort field
                if (order.tupleOrder == TupleOrder.Ascending) {
                    try {
                        MIN_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                    } catch (Exception e) {
                        throw new SortException(e, "MIN_VAL failed");
                    }
                } else {
                    try {
                        MAX_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                    } catch (Exception e) {
                        throw new SortException(e, "MIN_VAL failed");
                    }
                }

                // switch the current heap and the other heap
                pnodeSplayPQ tempQ = pcurr_Q;
                pcurr_Q = pother_Q;
                pother_Q = tempQ;
                int tempelems = p_elems_curr_Q;
                p_elems_curr_Q = p_elems_other_Q;
                p_elems_other_Q = tempelems;
            }

            // now check whether the current queue is empty
            else if (p_elems_curr_Q == 0) {
                while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
                    try {
                        tuple = _am.get_next();  // according to Iterator.java
                    } catch (Exception e) {
                        throw new SortException(e, "get_next() failed");
                    }

                    if (tuple == null) {
                        break;
                    }
                    cur_node = new pnode();
                    cur_node.tuple = new Tuple(tuple); // tuple copy needed --  Bingjie 4/29/98

                    try {
                        pcurr_Q.enq(cur_node);
                    } catch (UnknowAttrType e) {
                        throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                    }
                    p_elems_curr_Q++;
                }
            }

            // Check if we are done
            if (p_elems_curr_Q == 0) {
                // current queue empty despite our attemps to fill in
                // indicating no more tuples from input
                if (p_elems_other_Q == 0) {
                    // other queue is also empty, no more tuples to write out, done
                    break; // of the while(true) loop
                } else {
                    // generate one more run for all tuples in the other queue
                    // close current run and start next run
                    n_tuples.add((int) o_buf.flush());  // need io_bufs.java
                    run_num++;
                    try {
                        temp_files.get(run_num-1).finishSequentialInsert();
                        temp_files.add(new RunFile());
                        temp_files.get(run_num).initiateSequentialInsert();
                    } catch (Exception e) {
                        throw new SortException(e, "Sort.java: create Heapfile failed");
                    }

                    // need io_bufs.java
                    o_buf.init(bufs, _n_pages-1, tuple_size, temp_files.get(run_num), false);

                    // set the last Elem to be the minimum value for the sort field
                    if (order.tupleOrder == TupleOrder.Ascending) {
                        try {
                            MIN_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                        } catch (Exception e) {
                            throw new SortException(e, "MIN_VAL failed");
                        }
                    } else {
                        try {
                            MAX_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                        } catch (Exception e) {
                            throw new SortException(e, "MIN_VAL failed");
                        }
                    }

                    // switch the current heap and the other heap
                    pnodeSplayPQ tempQ = pcurr_Q;
                    pcurr_Q = pother_Q;
                    pother_Q = tempQ;
                    int tempelems = p_elems_curr_Q;
                    p_elems_curr_Q = p_elems_other_Q;
                    p_elems_other_Q = tempelems;
                }
            } // end of if (p_elems_curr_Q == 0)
        } // end of while (true)

        // close the last run
        n_tuples.add((int) o_buf.flush());
        temp_files.get(run_num).finishSequentialInsert();
        run_num++;
        free_buffer_pages(_n_pages-1,bufs_pids);
        return run_num;
    }

    /**
     * Set lastElem to be the minimum value of the appropriate type
     *
     * @param lastElem    the tuple
     * @param sortFldType the sort field type
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MIN_VAL(Tuple lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
        //    AttrType[] junk = new AttrType[1];
        //    junk[0] = new AttrType(sortFldType.attrType);
        char[] c = new char[1];
        c[0] = Character.MIN_VALUE;
        String s = new String(c);
        //    short fld_no = 1;

        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
                break;
            case AttrType.attrReal:
                //      lastElem.setHdr(fld-no, junk, null);
                lastElem.setFloFld(_sort_fld, Float.MIN_VALUE);
                break;
            case AttrType.attrString:
                //      lastElem.setHdr(fld_no, junk, s_size);
                lastElem.setStrFld(_sort_fld, s);
                break;
            default:
                // don't know how to handle attrSymbol, attrNull
                //System.err.println("error in sort.java");
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

        return;
    }

    /**
     * Set lastElem to be the maximum value of the appropriate type
     *
     * @param lastElem    the tuple
     * @param sortFldType the sort field type
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MAX_VAL(Tuple lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
        //    AttrType[] junk = new AttrType[1];
        //    junk[0] = new AttrType(sortFldType.attrType);
        char[] c = new char[1];
        c[0] = Character.MAX_VALUE;
        String s = new String(c);
        //    short fld_no = 1;

        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
                break;
            case AttrType.attrReal:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setFloFld(_sort_fld, Float.MAX_VALUE);
                break;
            case AttrType.attrString:
                //      lastElem.setHdr(fld_no, junk, s_size);
                lastElem.setStrFld(_sort_fld, s);
                break;
            default:
                // don't know how to handle attrSymbol, attrNull
                //System.err.println("error in sort.java");
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

        return;
    }

    /**
     * Returns the next tuple in sorted order.
     * Note: You need to copy out the content of the tuple, otherwise it
     * will be overwritten by the next <code>get_next()</code> call.
     *
     * @return the next tuple, null if all tuples exhausted
     * @throws IOException     from lower layers
     * @throws SortException   something went wrong in the lower layer.
     * @throws JoinsException  from <code>generate_runs()</code>.
     * @throws UnknowAttrType  attribute type unknown
     * @throws LowMemException memory low exception
     * @throws Exception       other exceptions
     */
    public Tuple get_next()
            throws IOException,
            SortException,
            UnknowAttrType,
            LowMemException,
            JoinsException,
            Exception {
        if (first_time) {
            // first get_next call to the sort routine
            first_time = false;

            // generate runs
            Nruns = generate_runs(max_elems_in_heap, _in[_sort_fld - 1]);

            // setup state to perform merge of runs.
            // Open input buffers for all the input file
            setup_for_merge(tuple_size);
            for(int i = 0; i < temp_files.size(); i++)
                if(temp_files.get(i) != null) {
                    tuples[i] = null;
                    temp_files.get(i).initiateScan();
                }
        }
        int size = temp_files.size();
        int i = 0;
        Tuple t = new Tuple(tuple_size);
        t.setHdr(n_cols, _in, str_lens);
        if (order.tupleOrder == TupleOrder.Ascending)
            MAX_VAL(t, _in[_sort_fld - 1]);
        else
            MIN_VAL(t, _in[_sort_fld - 1]);
        int k = -1;
        int j = i;
        for (; j < i + _n_pages; j++) {
            if (j < size && temp_files.get(j) != null) {
                if(tuples[j]==null)
                    tuples[j] = temp_files.get(j).getNext();
                if (tuples[j] != null) {
                    tuples[j].setHdr(n_cols, _in, str_lens);
                    int ans = TupleUtils.CompareTupleWithTuple(_in[_sort_fld - 1], tuples[j], _sort_fld, t, _sort_fld);
                    if ((order.tupleOrder == TupleOrder.Ascending && ans == -1) ||
                            (order.tupleOrder == TupleOrder.Descending && ans == 1)){
                        t = new Tuple(tuples[j]);
                        k = j;
                    }
                }
            }
        }
        if (k > -1) {
            tuples[k] = null;
            /*int l = i;
            for (; l < i +_n_pages; l++) {
                if (l < size && l != k && temp_files.get(l)!= null) {
                    temp_files.get(l).setPrev();
                }
            }*/
        } else {
            passes++;
            t = null;
        }
        //Tuple output_tuple = temp_files.get(0).getNext();
        if(t != null){
            t.setHdr(n_cols, _in, str_lens);
            return t;
        }
        return null;
    }

    public int getPasses() {
        return passes;
    }

    /**
     * Cleaning up, including releasing buffer pages from the buffer pool
     * and removing temporary files from the database.
     *
     * @throws IOException   from lower layers
     * @throws SortException something went wrong in the lower layer.
     */
    public void close() throws SortException, IOException {
        // clean up
        if (!closeFlag) {

            try {
                _am.close();
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: error in closing iterator.");
            }

            for (int i = 0; i < temp_files.size(); i++) {
                if (temp_files.get(i) != null) {
                    try {
                        temp_files.get(i).finishScan();
                        temp_files.get(i).deleteFile();
                    } catch (Exception e) {
                        throw new SortException(e, "Sort.java: Heapfile error");
                    }
                    temp_files.set(i, null);
                }
            }
            closeFlag = true;
        }
    }

}


