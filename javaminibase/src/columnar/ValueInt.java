package columnar;

public class ValueInt<Integer> extends ValueClass {

    public ValueInt(Integer val) {
        value = val;
    }

    public Integer getValue() {
        return (Integer)value;
    }

    public String toString() {
        return String.valueOf(value);
    }
}