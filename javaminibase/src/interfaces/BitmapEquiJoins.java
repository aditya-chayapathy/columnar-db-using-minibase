package interfaces;

import columnar.ColumnarBitmapEquiJoinsII;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

public class BitmapEquiJoins {

    public static void main(String[] args) throws Exception {
        // Query Skeleton: COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST EQUICONST [TARGETCOLUMNS] NUMBUF
        // Example Query: testColumnDB columnarTable1 columnarTable2 "([columnarTable1.A = 10] v [columnnarTable1.B > 2]) ^ ([columnnarTable1.C = 20])" "([columnarTable2.A = 10] v [columnarTable2.B > 2]) ^ ([columnarTable2.C = 20])" "([columnarTable1.A = columnarTable2.A] v [columnarTable1.B = columnarTable2.B]) ^ ([columnarTable1.C = columnarTable2.C])" columnarTable1.A,columnarTable1.B,columnarTable2.C,columnarTable2.D 100
        // In case no constraints need to be applied, pass "" as input.
        //test R1 R2 "([R1.X = 10] v [R1.B > 2])" "([R2.C = 20])" "([R1.A = R2.C]) ^ ([R1.B = R2.D])" R1.A,R1.B,R2.C,R2.D 100

        //test R1 R2 "[R1.A > 2])" "([R2.C < 20])" "([R1.A = R2.C] v [R1.B = R2.D])" R1.A,R1.B,R2.C,R2.D 100

        //test R1 R2 "[R1.A > 2])" "([R2.D < 20])" "([R1.A = R2.D]) ^ ([R1.B = R2.E]) ^ ([R1.C = R2.F])" R1.A,R1.B,R2.D,R2.E 100
        //test R1 R2 "[R1.A > 2])" "([R2.D < 20])" "([R1.A = R2.D] v [R1.B = R2.E]) ^ ([R1.C = R2.F])" R1.A,R1.B,R2.D,R2.E 100

        //test R1 R2 "" "" "([R1.A = R2.C]) ^ ([R1.B = R2.D]) ^ ([R1.X = R2.Y])" R1.A,R1.B,R1.X,R2.C,R2.Y 100

        String columnDB = args[0];
        String outerColumnarFile = args[1];
        String innerColumnarFile = args[2];
        String rawOuterConstraint = args[3];
        String rawInnerConstraint = args[4];
        String rawEquijoinConstraint = args[5];
        String[] targetColumns = args[6].split(",");
        Integer bufferSize = Integer.parseInt(args[7]);

        String dbpath = InterfaceUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        runInterface(outerColumnarFile, innerColumnarFile, rawOuterConstraint, rawInnerConstraint, rawEquijoinConstraint, targetColumns);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String outerColumnarFile, String innerColumnarFile, String rawOuterConstraint, String rawInnerConstraint, String rawEquijoinConstraint, String[] targetColumns) throws Exception {
        Columnarfile outer = new Columnarfile(outerColumnarFile);
        Columnarfile inner = new Columnarfile(innerColumnarFile);

        CondExpr[] innerColumnarConstraint = InterfaceUtils.processRawConditionExpression(rawInnerConstraint, inner);
        CondExpr[] outerColumnarConstraint = InterfaceUtils.processRawConditionExpression(rawOuterConstraint, outer);
        CondExpr[] equiJoinConstraint = InterfaceUtils.processEquiJoinConditionExpression(rawEquijoinConstraint, inner, outer);

        AttrType[] opAttr = new AttrType[targetColumns.length];
        FldSpec[] projectionList = new FldSpec[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++) {
            String attribute = targetColumns[i].split("\\.")[1];
            String relationName = targetColumns[i].split("\\.")[0];
            if (relationName.equals(outerColumnarFile)) {
                projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), outer.getAttributePosition(attribute) + 1);
                opAttr[i] = new AttrType(outer.getAttrtypeforcolumn(outer.getAttributePosition(attribute)).attrType);
            } else {
                projectionList[i] = new FldSpec(new RelSpec(RelSpec.innerRel), inner.getAttributePosition(attribute) + 1);
                opAttr[i] = new AttrType(inner.getAttrtypeforcolumn(inner.getAttributePosition(attribute)).attrType);
            }
        }

        // Call the equijoin interface
        /*
        *
        * AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            AttrType[] in2,
            int len_in2,
            short[] t2_str_sizes,
            int amt_of_mem,
            String leftColumnarFileName,
            int leftJoinField,
            String rightColumnarFileName,
            int rightJoinField,
            FldSpec[] proj_list,
            int n_out_flds,
            CondExpr[] joinExp,
            CondExpr[] innerExp,
            CondExpr[] outerExp
        * */

        ColumnarBitmapEquiJoinsII columnarBitmapEquiJoins = new ColumnarBitmapEquiJoinsII(outer.getAttributes(),
                outer.getnumColumns(), outer.getAttrSizes(),
                inner.getAttributes(),
                inner.getnumColumns(),
                inner.getAttrSizes(),
                2,
                outerColumnarFile,
                -1,
                innerColumnarFile,
                -1,
                projectionList,
                targetColumns.length,
                equiJoinConstraint,
                innerColumnarConstraint, outerColumnarConstraint, opAttr);

        outer.close();
        inner.close();
    }
}
