package btree;

import global.AttrType;
import global.Convert;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;

import java.io.IOException;

public class KeyFactory {
    public static KeyClass getKeyClass(byte[] data, AttrType type, short size) throws IOException {

        KeyClass value = null;
        switch (type.attrType) {
            case 0:
                String s = Convert.getStrValue(0, data, size);
                value = new StringKey(s);
                break;
            case 1:
                Integer i = Convert.getIntValue(0, data);
                value = new IntegerKey(i);
                break;
            case 2:
                Float f = Convert.getFloValue(0, data);
                value = new FloatKey(f);
                break;
        }

        return value;
    }

    public static KeyClass getKeyClass(Tuple tuple, AttrType type) throws IOException, FieldNumberOutOfBoundException {

        KeyClass value = null;
        switch (type.attrType) {
            case 0:
                value = new StringKey(tuple.getStrFld(1));
                break;
            case 1:
                value = new IntegerKey(tuple.getIntFld(1));
                break;
            case 2:
                value = new FloatKey(tuple.getFloFld(1));
                break;
        }

        return value;
    }
}
