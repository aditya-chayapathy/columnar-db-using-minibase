package heap;

import chainexception.ChainException;

public class InvalidUpdateException extends ChainException {


    public InvalidUpdateException() {
        super();
    }

    public InvalidUpdateException(Exception ex, String name) {
        super(ex, name);
    }


}
