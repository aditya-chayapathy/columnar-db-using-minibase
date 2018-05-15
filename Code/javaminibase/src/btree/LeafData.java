package btree;

import global.RID;

/**
 * IndexData: It extends the DataClass.
 * It defines the data "rid" for leaf node in B++ tree.
 */
public class LeafData extends DataClass {
    private int myPosition;

    /**
     * Class constructor
     *
     * @param rid the data rid
     */
    LeafData(int position) {
        myPosition = position;
    }

    public String toString() {
        String s;
        s = "[ " + Integer.toString(myPosition) + " ]";
        return s;
    }

    ;

    /**
     * get a copy of the rid
     *
     * @return the reference of the copy
     */
    public int getData() {
        return myPosition;
    }

    ;

    /**
     * set the rid
     */
    public void setData(int position) {
        myPosition = position;
    }

    ;
}   
