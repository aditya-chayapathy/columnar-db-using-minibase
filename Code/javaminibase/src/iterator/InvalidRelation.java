package iterator;

import chainexception.ChainException;

public class InvalidRelation extends ChainException {
    public InvalidRelation(String s) {
        super(null, s);
    }

    public InvalidRelation(Exception prev, String s) {
        super(prev, s);
    }
}
