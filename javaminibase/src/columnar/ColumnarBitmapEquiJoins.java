package columnar;

import bitmap.BM;
import bitmap.BitMapFile;
import bitmap.BitMapHeaderPage;
import global.AttrType;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.PredEval;

import java.util.*;
import java.util.stream.Collectors;

//test R1 R2 "([R1.X = 10] v [R1.B > 2])" "([R2.C = 20])" "([R1.A = R2.C]) ^ ([R1.B = R1.D])" R1.A,R1.B,R2.C,R2.D 100
//test R1 R2 "([R1.B > 2])" "([R2.C = 20])" "([R1.A = R2.C]) ^ ([R1.B = R1.D])" R1.A,R1.B,R2.C,R2.D 100
//test R1 R2 "([R1.X = A])" "([R2.C = 20])" "([R1.A = R2.C]) ^ ([R1.B = R1.D])" R1.A,R1.B,R2.C,R2.D 100

//todo check writes?

public class ColumnarBitmapEquiJoins  {
    private final Columnarfile leftColumnarFile;
    private final Columnarfile rightColumnarFile;
    private int offset1;
    private int offset2;
    private List<String> joinConditions = new ArrayList<>();
    // contains two lists with R1 and R2 offsets
    private List<List<Integer>> offsets = new ArrayList<>();
    // need to change to ValueClass

    public ColumnarBitmapEquiJoins(
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
            CondExpr[] outerExp) throws Exception {


        assert innerExp.length == 1;
        assert outerExp.length == 1;

        leftColumnarFile = new Columnarfile(leftColumnarFileName);
        rightColumnarFile = new Columnarfile(rightColumnarFileName);

        //
        offsets.add(new ArrayList<>());
        offsets.add(new ArrayList<>());

        List<HashSet<String>> uniSet = getUniqueSetFromJoin(joinExp, leftColumnarFile, rightColumnarFile);
        List<List<String>> combinations = getSubs(uniSet);


        List<BitSet> addedCombinationsOfR1 = getEquiJoinRelation(combinations, leftColumnarFile);
        List<BitSet> addedCombinationsOfR2 = getEquiJoinRelation(combinations, rightColumnarFile);

        List<List<Integer>> r1Positions = new ArrayList<>();

        for (int i = 0; i < addedCombinationsOfR1.size(); i++) {
            BitSet currentBitSet = addedCombinationsOfR1.get(i);
            r1Positions.add(new ArrayList<>());
            for(int k = 0; k < currentBitSet.length(); k++) {
                if(currentBitSet.get(k)) {
                    r1Positions.get(i).add(k);
                }
            }
        }

        List<List<Integer>> r2Positions = new ArrayList<>();
        for (int i = 0; i < addedCombinationsOfR2.size(); i++) {
            BitSet currentBitSet = addedCombinationsOfR2.get(i);
            r2Positions.add(new ArrayList<>());
            for(int k = 0; k < currentBitSet.length(); k++) {
                if(currentBitSet.get(k)) {
                    r2Positions.get(i).add(k);
                }
            }
        }

        System.out.println(r1Positions);
        System.out.println(r2Positions);


         for(int i = 0; i < r1Positions.size(); i++) {
            List<Integer> leftRelation = r1Positions.get(i);
            List<Integer> rightRelation = r2Positions.get(i);
            List<List<Integer>> entries = new ArrayList<>();

            entries.add(leftRelation);
            entries.add(rightRelation);
            List<List<Integer>> entriesAfterJoin = nestedLoop(entries);


            for(int k =0; k < entriesAfterJoin.size(); k++) {
                Tuple tuple = leftColumnarFile.getTuple(entriesAfterJoin.get(k).get(0));
                tuple.print(leftColumnarFile.getAttributes());

                Tuple tuple1 = rightColumnarFile.getTuple(entriesAfterJoin.get(k).get(1));
                tuple1.print(rightColumnarFile.getAttributes());


                if(PredEval.Eval(outerExp, tuple, null, leftColumnarFile.getAttributes(), null)) {
                    if(PredEval.Eval(innerExp, tuple, null, rightColumnarFile.getAttributes(), null)) {
                        //todo
                    }
                }
            }


        }

        System.out.println(joinConditions);
    }

    private List<BitSet> getEquiJoinRelation(List<List<String>> combinations, Columnarfile columnarfile) throws Exception {
        List<BitSet> addedCombinations = new ArrayList<>();

        for(List<String> combination: combinations) {
            List<BitSet> bitSets = new ArrayList<>();
            for (int i = 0; i < combination.size(); i++) {
                String bmName = columnarfile.getBMName(offsets.get(0).get(i) - 1, new ValueString<>(combination.get(i)));
                BitMapFile bitMapFile = columnarfile.getBMIndex(bmName);

                //todo need to discuss this as header page is null
                bitMapFile.setHeaderPage(new BitMapHeaderPage(bitMapFile.getHeaderPageId()));


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

                //todo move this is to interface we assume that the colums already have indexes
                leftColumnarFile.createAllBitMapIndexForColumn(offset1 - 1);

                HashMap<String, BitMapFile> allBitMaps = leftColumnarFile.getAllBitMaps();

                FldSpec symbol2 = currentCondition.operand2.symbol;
                offset2 = symbol2.offset;
                offsets.get(1).add(offset2);

                //todo move this is to interface we assume that the colums already have indexes
                rightColumnarFile.createAllBitMapIndexForColumn(offset2 -1);

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
