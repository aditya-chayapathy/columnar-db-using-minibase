package iterator;

import chainexception.ChainException;

public class LowMemException extends ChainException {
    public LowMemException(String s) {
        super(null, s);
    }

    public LowMemException(Exception e, String s) {
        super(e, s);
    }
}
