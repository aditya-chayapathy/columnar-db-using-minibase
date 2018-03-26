package btree;

import chainexception.ChainException;

public class RecordNotFoundException extends ChainException {
    public RecordNotFoundException() {
        super();
    }

    public RecordNotFoundException(String s) {
        super(null, s);
    }

    public RecordNotFoundException(Exception e, String s) {
        super(e, s);
    }

}
