package diskmgr;

import chainexception.ChainException;

public class InvalidRunSizeException extends ChainException {

    public InvalidRunSizeException(Exception e, String name) {
        super(e, name);
    }
}




