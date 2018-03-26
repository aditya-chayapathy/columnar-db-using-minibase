package btree;

import chainexception.ChainException;

public class GetFileEntryException extends ChainException {
    public GetFileEntryException() {
        super();
    }

    public GetFileEntryException(String s) {
        super(null, s);
    }

    public GetFileEntryException(Exception e, String s) {
        super(e, s);
    }

}
