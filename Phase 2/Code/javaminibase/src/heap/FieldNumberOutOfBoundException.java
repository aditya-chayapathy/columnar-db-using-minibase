package heap;

import chainexception.ChainException;

public class FieldNumberOutOfBoundException extends ChainException {

    public FieldNumberOutOfBoundException() {
        super();
    }

    public FieldNumberOutOfBoundException(Exception ex, String name) {
        super(ex, name);
    }

}

