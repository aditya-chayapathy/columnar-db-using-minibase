package iterator;

import chainexception.ChainException;

public class WrongPermat extends ChainException {
    public WrongPermat(String s) {
        super(null, s);
    }

    public WrongPermat(Exception prev, String s) {
        super(prev, s);
    }
}
