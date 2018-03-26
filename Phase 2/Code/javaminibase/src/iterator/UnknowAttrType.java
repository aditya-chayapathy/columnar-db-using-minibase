package iterator;

import chainexception.ChainException;

public class UnknowAttrType extends ChainException {
    public UnknowAttrType(String s) {
        super(null, s);
    }

    public UnknowAttrType(Exception prev, String s) {
        super(prev, s);
    }
}
