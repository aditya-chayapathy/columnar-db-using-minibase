package heap;

import chainexception.ChainException;


public class InvalidTypeException extends ChainException {


    public InvalidTypeException() {
        super();
    }

    public InvalidTypeException(Exception ex, String name) {
        super(ex, name);
    }


}
