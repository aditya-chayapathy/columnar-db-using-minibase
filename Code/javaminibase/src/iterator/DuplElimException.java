package iterator;

import chainexception.ChainException;

public class DuplElimException extends ChainException {
    public DuplElimException(String s) {
        super(null, s);
    }

    public DuplElimException(Exception prev, String s) {
        super(prev, s);
    }
}
