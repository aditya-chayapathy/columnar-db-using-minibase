package iterator;

import chainexception.ChainException;

public class SortException extends ChainException {
    public SortException(String s) {
        super(null, s);
    }

    public SortException(Exception e, String s) {
        super(e, s);
    }
}
