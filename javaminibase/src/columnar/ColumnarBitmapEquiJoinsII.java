package columnar;

import bitmap.BM;
import bitmap.BitMapFile;
import global.AttrType;
import heap.Tuple;
import iterator.*;

import java.util.*;
import java.util.stream.Collectors;

//test R1 R2 "([R1.X = 10] v [R1.B > 2])" "([R2.C = 20])" "([R1.A = R2.C]) ^ ([R1.B = R1.D])" R1.A,R1.B,R2.C,R2.D 100
//test R1 R2 "([R1.B > 2])" "([R2.C = 20])" "([R1.A = R2.C]) ^ ([R1.B = R1.D])" R1.A,R1.B,R2.C,R2.D 100
//test R1 R2 "([R1.X = A])" "([R2.C = 20])" "([R1.A = R2.C]) ^ ([R1.B = R1.D])" R1.A,R1.B,R2.C,R2.D 100
//test R1 R2 "[R1.A > 2])" "([R2.C < 20])" "([R1.A = R2.C]) ^ ([R1.B = R2.D])" R1.A,R1.B,R2.C,R2.D 100
//todo check writes?

public class ColumnarBitmapEquiJoinsII {
    private final Columnarfile leftColumnarFile;
    private final Columnarfile rightColumnarFile;
    private int offset1;
    private int offset2;
    private List<String> joinConditions = new ArrayList<>();
    // contains two lists with R1 and R2 offsets
    private List<List<Integer>> offsets = new ArrayList<>();
    // need to change to ValueClass

    /**
     *
     * @param in1
     * @param len_in1
     * @param t1_str_sizes
     * @param in2
     * @param len_in2
     * @param t2_str_sizes
     * @param amt_of_mem
     * @param leftColumnarFileName
     * @param leftJoinField
     * @param rightColumnarFileName
     * @param rightJoinField
     * @param proj_list
     * @param n_out_flds
     * @param joinExp
     * @param innerExp
     * @param outerExp
     * @param opAttr
     * @throws Exception
     */
    public ColumnarBitmapEquiJoinsII(
            AttrType[] in1,
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
            CondExpr[] outerExp, AttrType[] opAttr) throws Exception {


        assert innerExp.length == 1;
        assert outerExp.length == 1;

        leftColumnarFile = new Columnarfile(leftColumnarFileName);
        rightColumnarFile = new Columnarfile(rightColumnarFileName);

        //
        offsets.add(new ArrayList<>());
        offsets.add(new ArrayList<>());

        List<HashSet<String>> uniSet = getUniqueSetFromJoin(joinExp, leftColumnarFile, rightColumnarFile);


        List<List<String>> positions = new ArrayList<>();

        for(int i =0; i < uniSet.size(); i++) {
            HashSet<String> unisets = uniSet.get(i);


            List<String> predicatePositions = new ArrayList<>();
            for(String eachUniqueSet: unisets ) {

                String leftBMname = leftColumnarFile.getBMName(offsets.get(0).get(i) - 1, new ValueString<>(eachUniqueSet));
                BitMapFile leftBitMapfile = new BitMapFile(leftBMname);

                String rightBmName = rightColumnarFile.getBMName(offsets.get(0).get(i) - 1, new ValueString<>(eachUniqueSet));
                BitMapFile rightBitMapfile = new BitMapFile(rightBmName);

                BitSet leftBitMaps = BM.getBitMap(leftBitMapfile.getHeaderPage());
                BitSet rightBitMaps = BM.getBitMap(rightBitMapfile.getHeaderPage());


                List<Integer> r1Positions = new ArrayList<>();
                List<Integer> r2Positions = new ArrayList<>();

                for(int k = 0; k < leftBitMaps.size(); k++) {
                    if(leftBitMaps.get(k)) {
                        r1Positions.add(k);
                    }
                }

                for(int k = 0; k < rightBitMaps.size(); k++) {
                    if(rightBitMaps.get(k)) {
                        r2Positions.add(k);
                    }
                }

                List<List<Integer>> entries = new ArrayList<>();
                entries.add(r1Positions);
                entries.add(r2Positions);
                List<List<Integer>> lists = nestedLoop(entries);


                for (List<Integer> list : lists) {

                    predicatePositions.add(list.get(0) + "#" + list.get(1));
                }
            }

            positions.add(predicatePositions);

        }

        HashSet<String> resultTillNow = new HashSet<>(positions.get(0));

        List<HashSet<String>> newList = new ArrayList<>();

        for(int i = 0, j = 1; i < joinConditions.size(); i++, j++) {
            if(joinConditions.get(i).equals("OR")) {
                resultTillNow.addAll(positions.get(j));
            } else {
                newList.add(resultTillNow);
                resultTillNow = new HashSet<>(positions.get(j));
            }
        }
        newList.add(resultTillNow);

        resultTillNow = newList.get(0);

        for (int i=1; i < newList.size(); i++) {

            resultTillNow.retainAll(newList.get(i));
        }

        for(String positionsAfterJoin: resultTillNow) {
            String[] split = positionsAfterJoin.split("#");

            Tuple tuple = leftColumnarFile.getTuple(Integer.parseInt(split[0]));

            if(PredEval.Eval(outerExp, tuple, null, leftColumnarFile.getAttributes(), null)) {
                Tuple tuple1 = rightColumnarFile.getTuple(Integer.parseInt(split[1]));
                if(PredEval.Eval(innerExp, tuple1, null, rightColumnarFile.getAttributes(), null)) {

                    Tuple Jtuple = new Tuple();
                    AttrType[] Jtypes=new AttrType[n_out_flds];

                    TupleUtils.setup_op_tuple(Jtuple,Jtypes,in1,len_in1,in2,len_in2,t1_str_sizes,t2_str_sizes,proj_list,n_out_flds);

                    Projection.Join(tuple, in1,
                            tuple1, in2,
                            Jtuple, proj_list, n_out_flds);
                    Jtuple.print(opAttr);
                }
            }
        }
    }

    private List<BitSet> getEquiJoinRelation(List<List<String>> combinations, Columnarfile columnarfile) throws Exception {
        List<BitSet> addedCombinations = new ArrayList<>();

        for(List<String> combination: combinations) {
            List<BitSet> bitSets = new ArrayList<>();
            for (int i = 0; i < combination.size(); i++) {
                String bmName = columnarfile.getBMName(offsets.get(0).get(i) - 1, new ValueString<>(combination.get(i)));
                BitMapFile bitMapFile = new BitMapFile(bmName);

                BitSet bitMaps = BM.getBitMap(bitMapFile.getHeaderPage());
                bitSets.add(bitMaps);
            }

            List<BitSet> oRedBitmaps = new ArrayList<>();

            BitSet resultTillNow = bitSets.get(0);
            List<BitSet> newList = new ArrayList<>();
            for(int i = 0, j = 1; i < joinConditions.size(); i++, j++) {
                if(joinConditions.get(i).equals("OR")) {
                    resultTillNow.or(bitSets.get(j));
                } else {
                    newList.add(resultTillNow);
                    resultTillNow = bitSets.get(j);
                }
            }
            newList.add(resultTillNow);

            resultTillNow = newList.get(0);
            for (int i=1; i < newList.size(); i++) {
                resultTillNow.and(newList.get(i));
            }

            addedCombinations.add(resultTillNow);
        }

        return addedCombinations;
    }


    private List<HashSet<String>> getUniqueSetFromJoin(CondExpr[] joinEquation, Columnarfile leftColumnarFile,
                                               Columnarfile rightColumnarFile) throws Exception {

        List<HashSet<String>> uniquesList = new ArrayList<>();

        for(int i = 0; i < joinEquation.length; i++) {

            CondExpr currentCondition = joinEquation[i];

            while(currentCondition != null) {

                FldSpec symbol = currentCondition.operand1.symbol;
                offset1 = symbol.offset;

                offsets.get(0).add(offset1);

                //todo remove before submission
//                leftColumnarFile.createAllBitMapIndexForColumn(offset1 - 1);

                HashMap<String, BitMapFile> allBitMaps = leftColumnarFile.getAllBitMaps();

                FldSpec symbol2 = currentCondition.operand2.symbol;
                offset2 = symbol2.offset;
                offsets.get(1).add(offset2);

                //todo remove before submission
//                rightColumnarFile.createAllBitMapIndexForColumn(offset2 -1);

                HashSet<String> set1 = extractUniqueValues(offset1 - 1, allBitMaps);
                HashMap<String, BitMapFile> allRightRelationBitMaps = rightColumnarFile.getAllBitMaps();


                HashSet<String> set2 = extractUniqueValues(offset2 -1, allRightRelationBitMaps);

                set1.retainAll(set2);
                uniquesList.add(set1);

                currentCondition = currentCondition.next;
                if(currentCondition != null) {
                    joinConditions.add("OR"); // always joins are represented in CNF
                }
            }
            if(i!=0 && joinEquation[i] != null)  {
                joinConditions.add("AND");
            }
        }

        return uniquesList;
    }

    public HashSet<String> extractUniqueValues(int offset, HashMap<String, BitMapFile> allBitMaps) {

        HashSet<String> collect = allBitMaps.
                keySet()
                .stream()
                .filter(e -> {
                    String[] split = e.split("\\.");
                    if (Integer.parseInt(split[2]) == offset) {
                        return true;
                    }
                    return false;
                })
                .map(e -> e.split("\\.")[3])
                .collect(Collectors.toCollection(HashSet::new));

        return collect;
    }


    public List<List<String>> getSubs(List<HashSet<String>> uniqueSets)  {
        List<List<String>> res = new ArrayList<>();
        bt(uniqueSets, 0,res, new ArrayList<>());
        return res;
    }

    private void bt(List<HashSet<String>> uniqueSets, int index, List<List<String>> res, List<String> path) {

        if(path.size() == uniqueSets.size()) {
            ArrayList<String> k = new ArrayList<>(path);
            res.add(k);
            return;
        }

        HashSet<String> uniqueSet = uniqueSets.get(index);
        for(String entry: uniqueSet) {
            path.add(entry);
            bt(uniqueSets, index+1, res, path);
            path.remove(path.size() - 1);
        }
    }


    public List<List<Integer>> nestedLoop(List<List<Integer>> uniqueSets)  {
        List<List<Integer>> res = new ArrayList<>();
        nestedLoopBt(uniqueSets, 0,res, new ArrayList<>());
        return res;
    }

    private void nestedLoopBt(List<List<Integer>> uniqueSets, int index, List<List<Integer>> res, List<Integer> path) {

        if(path.size() == uniqueSets.size()) {
            ArrayList<Integer> k = new ArrayList<>(path);
            res.add(k);
            return;
        }

        List<Integer> uniqueSet = uniqueSets.get(index);
        for(Integer entry: uniqueSet) {
            path.add(entry);
            nestedLoopBt(uniqueSets, index+1, res, path);
            path.remove(path.size() - 1);
        }
    }


}
