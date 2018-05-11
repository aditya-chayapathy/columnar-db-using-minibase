package btree;

import chainexception.ChainException;

public class LeafDeleteException extends ChainException {
    public LeafDeleteException() {
        super();
    }

    public LeafDeleteException(Exception e, String s) {
        super(e, s);
    }

}
