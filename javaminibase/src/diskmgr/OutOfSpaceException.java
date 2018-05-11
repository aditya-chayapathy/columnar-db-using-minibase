package diskmgr;

import chainexception.ChainException;

public class OutOfSpaceException extends ChainException {

    public OutOfSpaceException(Exception e, String name) {
        super(e, name);
    }
}

