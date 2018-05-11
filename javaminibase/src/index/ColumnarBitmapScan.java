package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import bitmap.BitmapFileScan;
import btree.PinPageException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class ColumnarBitmapScan extends Iterator implements GlobalConst{

    private List<BitmapFileScan> scans;
    private Columnarfile columnarfile;
    private BitSet bitMaps;
    private int counter = 0;
    private int scanCounter = 0;

    private CondExpr[] _selects;
    private boolean index_only;
    private int _columnNo;

    public ColumnarBitmapScan(Columnarfile cf,
                              int columnNo,
                              CondExpr[] selects,
                              boolean indexOnly
    ) throws IndexException {

        _selects = selects;
        index_only = indexOnly;
        _columnNo = columnNo;
        try {

            columnarfile = cf;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            scans = new ArrayList<>();
            for (String bmName : columnarfile.getAvailableBM(columnNo)){
                if(evalBMName(bmName)){
                    scans.add((new BitMapFile(bmName)).new_scan());
                }
            }
        } catch (Exception e) {
            // any exception is swalled into a Index Exception
            throw new IndexException(e, "IndexScan.java: BitMapFile exceptions caught from BitMapFile constructor");
        }
    }

    @Override
    public Tuple get_next() throws IndexException, UnknownKeyTypeException {
        return get_next_BM();
    }

    public boolean delete_next() throws IndexException, UnknownKeyTypeException {

        return delete_next_BM();
    }

    public Tuple get_next_BM(){
        int position = 0;
        while (position != -1) {
            try {

                position = get_next_position();
                if (position < 0)
                    return null;
                // tuple that needs to sent
                Tuple JTuple = new Tuple(10);
                AttrType[] type = new AttrType[1];
                type[0] = new AttrType(AttrType.attrInteger);
                short[] sizes = new short[0];
                JTuple.setHdr((short)1, type, sizes);
                JTuple.setIntFld(1, position);
                return JTuple;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private boolean delete_next_BM() throws IndexException, UnknownKeyTypeException {
        int position = get_next_position();
        if(position < 0)
            return false;

        return columnarfile.markTupleDeleted(position);
    }

    public int get_next_position(){
        try {

            if (scanCounter == 0 || scanCounter > counter) {
                bitMaps = new BitSet();
                for(BitmapFileScan s : scans){
                    counter = s.counter;
                    BitSet bs = s.get_next_bitmap();
                    if(bs == null) {
                        return -1;
                    }
                    else {
                        bitMaps.or(bs);
                    }
                }
            }
            while (scanCounter <= counter) {
                if (bitMaps.get(scanCounter)) {
                    int position = scanCounter;
                    scanCounter++;
                    return position;
                } else {
                    scanCounter++;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return -1;
    }

    public void close() throws Exception {
        if (!closeFlag) {
            closeFlag = true;
            for(BitmapFileScan s : scans){
                s.close();
            }
        }
    }

    boolean evalBMName(String s) throws Exception {
        if(_selects == null)
            return true;

        short[] _sizes = new short[1];
        _sizes[0] = columnarfile.getAttrsizeforcolumn(_columnNo);
        AttrType[] _types = new AttrType[1];
        _types[0] = columnarfile.getAttrtypeforcolumn(_columnNo);

        byte[] data = new byte[6+_sizes[0]];
        String val = s.split("\\.")[3];
        if(_types[0].attrType == AttrType.attrInteger) {
            int t = Integer.parseInt(val);
            Convert.setIntValue(t,6, data);
        }else {
            Convert.setStrValue(val, 6, data);
        }
        Tuple jTuple = new Tuple(data,0,data.length);
        _sizes[0] -= 2;

        jTuple.setHdr((short)1,_types, _sizes);

        if(PredEval.Eval(_selects,jTuple,null,_types, null))
            return true;

        return false;
    }


}
