package columnar;

import global.AttrType;
import global.Convert;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;

import java.io.IOException;

public class ValueFactory {

    public static ValueClass getValueClass(byte[] data, AttrType type, short size) throws IOException {

        ValueClass value = null;
        switch (type.attrType) {
            case 0:
                String s = Convert.getStrValue(0, data, size);
                value = new ValueString(s);
                break;
            case 1:
                Integer i = Convert.getIntValue(0, data);
                value = new ValueInt(i);
                break;
            case 2:
                Float f = Convert.getFloValue(0, data);
                value = new ValueFloat(f);
                break;
        }

        return value;
    }

    public static ValueClass getValueClass(Tuple tuple, AttrType type) throws IOException, FieldNumberOutOfBoundException {

        ValueClass value = null;
        switch (type.attrType) {
            case 0:
                value = new ValueString(tuple.getStrFld(1));
                break;
            case 1:
                value = new ValueInt(tuple.getIntFld(1));
                break;
            case 2:
                value = new ValueFloat(tuple.getFloFld(1));
                break;
        }

        return value;
    }
}
