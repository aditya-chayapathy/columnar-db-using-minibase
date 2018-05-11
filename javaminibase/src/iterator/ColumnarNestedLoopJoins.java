package iterator;

import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import global.AttrType;
import heap.*;
import index.IndexException;

import java.io.IOException;

public class ColumnarNestedLoopJoins extends Iterator{
    private AttrType[] _in1,_in2;
    private Iterator outerIterator;
    private Tuple innerTuple, Jtuple, outerTuple;
    private CondExpr[] RightFilter, OutputFilter;
    private short[] InnertTargets;
    private Iterator innerScan;
    private boolean done,getfromouter;
    private FldSpec perm_mat[];
    private FldSpec i_proj[];
    private String cfName;

    public ColumnarNestedLoopJoins(AttrType[] in1, AttrType[] in2, Iterator am1,String relName, CondExpr[] outputFilter, CondExpr[] innerFilter, short[] innerTargets,FldSpec[] inner_Proj, FldSpec[] proj_list, Tuple proj_t){
        _in1 = in1;
        _in2 = in2;
        outerIterator = am1;
        innerTuple = new Tuple();
        Jtuple=proj_t;
        RightFilter = innerFilter;
        OutputFilter=outputFilter;
        InnertTargets = innerTargets;
        innerScan = null;
        done = false;
        getfromouter = true;
        perm_mat=proj_list;
        cfName = relName;
        i_proj = inner_Proj;
    }

    public Tuple get_next() throws Exception {
        if(done)
            return null;
        do{
            if(getfromouter==true){
                getfromouter=false;
                if(innerScan!=null){
                    innerScan=null;
                }
                try {
                    innerScan= new ColumnarFileScan(cfName, i_proj, InnertTargets, RightFilter);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if((outerTuple=outerIterator.get_next())==null){
                    done=true;
                    return null;
                }
            }

            while((innerTuple=innerScan.get_next())!=null) {
                if (PredEval.Eval(OutputFilter, outerTuple, innerTuple, _in1, _in2) == true) {
                    Projection.Join(outerTuple, _in1, innerTuple, _in2, Jtuple, perm_mat, perm_mat.length);
                    return Jtuple;
                }
            }
            getfromouter=true;
        }while(true);
    }

    public void close() throws JoinsException, IOException, IndexException {
        if (!closeFlag) {

            try {
                outerIterator.close();
                if(innerScan!= null)
                    innerScan.close();
            } catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
