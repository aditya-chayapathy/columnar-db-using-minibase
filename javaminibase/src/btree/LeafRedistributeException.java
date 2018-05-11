package btree;

import chainexception.ChainException;

public class LeafRedistributeException extends ChainException {
    public LeafRedistributeException() {
        super();
    }

    public LeafRedistributeException(String s) {
        super(null, s);
    }

    public LeafRedistributeException(Exception e, String s) {
        super(e, s);
    }

}
