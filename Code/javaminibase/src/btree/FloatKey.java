package btree;

/**
 * StringKey: It extends the KeyClass.
 * It defines the string Key.
 */
public class FloatKey extends KeyClass {

    private Float key;

    /**
     * Class constructor
     *
     * @param value the value of the float key to be set
     */
    public FloatKey(Float value) {
        key = new Float(value);
    }

    public String toString() {
        return key.toString();
    }

    /**
     * get a copy of the istring key
     *
     * @return the reference of the copy
     */
    public Float getKey() {
        return new Float(key);
    }

    /**
     * set the string key value
     */
    public void setKey(Float value) {
        key = value;
    }
}
