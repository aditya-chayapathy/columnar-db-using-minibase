package btree;

import chainexception.ChainException;

public class InsertException extends ChainException {
    public InsertException() {
        super();
    }

    public InsertException(String s) {
        super(null, s);
    }

    public InsertException(Exception e, String s) {
        super(e, s);
    }

}
