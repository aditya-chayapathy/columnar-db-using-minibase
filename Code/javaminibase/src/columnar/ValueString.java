package columnar;

public class ValueString<String> extends ValueClass {

    public ValueString(String val) {
        value = val;
    }

    public String getValue() {
        return (String) value;
    }

    public java.lang.String toString() {
        return java.lang.String.valueOf(value);
    }
}
