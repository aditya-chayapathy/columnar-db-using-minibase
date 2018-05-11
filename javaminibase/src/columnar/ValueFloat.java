package columnar;

public class ValueFloat<Float> extends ValueClass {

    public ValueFloat(Float val) {
        value = val;
    }

    public Float getValue() {
        return (Float)value;
    }

    public String toString() {
        return String.valueOf(value);
    }
}