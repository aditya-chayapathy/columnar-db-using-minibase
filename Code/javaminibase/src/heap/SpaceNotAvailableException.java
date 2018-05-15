package heap;

import chainexception.ChainException;

public class SpaceNotAvailableException extends ChainException {


    public SpaceNotAvailableException() {
        super();

    }

    public SpaceNotAvailableException(Exception ex, String name) {
        super(ex, name);
    }


}
