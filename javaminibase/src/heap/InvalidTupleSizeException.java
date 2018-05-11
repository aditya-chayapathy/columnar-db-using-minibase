package heap;

import chainexception.ChainException;

public class InvalidTupleSizeException extends ChainException {

    public InvalidTupleSizeException() {
        super();
    }

    public InvalidTupleSizeException(Exception ex, String name) {
        super(ex, name);
    }

}

