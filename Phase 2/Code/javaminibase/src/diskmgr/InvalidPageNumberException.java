package diskmgr;

import chainexception.ChainException;

public class InvalidPageNumberException extends ChainException {


    public InvalidPageNumberException(Exception ex, String name) {
        super(ex, name);
    }
}




