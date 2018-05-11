package iterator;

import bitmap.BMPage;
import bitmap.BitMapFile;
import btree.*;
import bufmgr.PageNotReadException;
import btree.PinPageException;
import columnar.Columnarfile;
import columnar.TID;
import columnar.ValueClass;
import diskmgr.Page;
import global.*;
import heap.*;
import index.ColumnarBTreeScan;
import index.ColumnarBitmapScan;
import index.UnknownIndexTypeException;
import iterator.*;
import org.w3c.dom.Attr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Set;


/**
 * Created by dixith on 3/18/18.
 */

public class ColumnarIndexScan extends Iterator{

    Iterator[] scan;
    private Heapfile[] targetHeapFiles = null;
    private AttrType[] targetAttrTypes = null;
    private short[] targetShortSizes = null;
    private short[] givenTargetedCols = null;
    private CondExpr[] _selects;
    private int index=0,max_pos=0;
    private Columnarfile columnarfile;
    private FldSpec[] perm_mat;
    private Tuple Jtuple;
    /*
    * relName: columnarfileName
    * columnNos: number of columns
    * indexTypes: for the corresponding columnNos
    * index_selects: Conditional expressions for columns which has index on it
    * selects: Conditional expressions for columns that has no index on them
    * indexOnly: true/false
    * targetedCols: Columns on which the conditions should be applied
    * proj_list: Output fields
    **/
    public ColumnarIndexScan(String relName,
                             int[] columnNos,
                             IndexType[] indexTypes,
                             CondExpr[][] index_selects,
                             CondExpr[] selects,
                             boolean indexOnly,
                             short[] targetedCols,
                             FldSpec[] proj_list, int mem) throws Exception {


        _selects = selects;
        scan= new Iterator[columnNos.length];
        perm_mat = proj_list;
        columnarfile = new Columnarfile(relName);
        givenTargetedCols = targetedCols;
        targetHeapFiles = ColumnarScanUtils.getTargetHeapFiles(columnarfile, targetedCols);
        targetAttrTypes = ColumnarScanUtils.getTargetColumnAttributeTypes(columnarfile, targetedCols);
        targetShortSizes = ColumnarScanUtils.getTargetColumnStringSizes(columnarfile, targetedCols);
        Jtuple = ColumnarScanUtils.getProjectionTuple(columnarfile, perm_mat, targetedCols);

        for(int i = 0; i < columnNos.length; i++) {
            switch (indexTypes[i].indexType) {
                case IndexType.B_Index:
                    Iterator im = new ColumnarBTreeScan(columnarfile, columnNos[i], index_selects[i], indexOnly);
                    if(columnNos.length == 1){
                        scan[i]= im;
                        break;
                    }
                    AttrType[] types = new AttrType[1];
                    types[0] = new AttrType(AttrType.attrInteger);
                    short[] sizes = new short[0];
                    scan[i] = new ColumnarSort(types, (short) 1, sizes, im, 1, new TupleOrder(TupleOrder.Ascending), 4, mem);
                    break;
                case IndexType.BitMapIndex:
                    scan[i] = new ColumnarBitmapScan(columnarfile, columnNos[i], index_selects[i], indexOnly);
                    break;
                case IndexType.None:
                default:
                    throw new UnknownIndexTypeException("Only BTree and Bitmap indices is supported so far");
            }
        }
    }

    @Override
    public Tuple get_next() throws Exception {
        int position = 0;
        Tuple t;
        while (position != -1) {
            try {
                position = get_next_position();
                if (position < 0)
                    return null;
                // tuple that needs to sent
                Tuple tTuple = new Tuple();
                // set the header which attribute types of the targeted columns
                tTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
                tTuple = new Tuple(tTuple.size());
                tTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
                for (int i = 0; i < targetHeapFiles.length; i++) {
                    Tuple record = targetHeapFiles[i].getRecord(position);
                    switch (targetAttrTypes[i].attrType) {
                        case AttrType.attrInteger:
                            // Assumed that col heap page will have only one entry
                            tTuple.setIntFld(i + 1,
                                    Convert.getIntValue(0, record.getTupleByteArray()));
                            break;
                        case AttrType.attrString:
                            tTuple.setStrFld(i + 1,
                                    Convert.getStrValue(0, record.getTupleByteArray(), targetShortSizes[i] + 2));
                            break;
                        default:
                            throw new Exception("Attribute indexAttrType not supported");
                    }
                }
                if (PredEval.Eval(_selects, tTuple, null, targetAttrTypes, null)) {
                    Projection.Project(tTuple, targetAttrTypes, Jtuple, perm_mat, perm_mat.length);
                    return Jtuple;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
        //return scan.get_next();
    }

    /*
    * get the first matching position in all the scans and return the satisfying position one by one
    * */
    public int get_next_position() throws Exception {
        /*iterate through all the scan objects*/
        int curr_max = -1, curr_index = -1, cnt = 0;
        Tuple t;
        while (true) {
            for (int i = 0; i < scan.length; i++) {
                if (i == curr_index)
                    continue;
                t = scan[i].get_next();
                if(t==null)
                    return -1;
                int p = t.getIntFld(1);
                if (p > curr_max) {
                    curr_max = p;
                    curr_index = i;
                    cnt = 1;
                } else if (p == curr_max) {
                    cnt++;
                }
            }
            if(cnt == scan.length)
                return curr_max;
        }
    }

    public boolean delete_next() throws Exception {

        /*switch (_index.indexType) {
            case IndexType.B_Index:
                return ((ColumnarBTreeScan)scan).delete_next();
            case IndexType.BitMapIndex:
                return ((ColumnarBitmapScan)scan).delete_next();
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }*/
        return true;
    }

    public void close(){
        try {
            for(int i=0;i<scan.length;i++)
            scan[i].close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
