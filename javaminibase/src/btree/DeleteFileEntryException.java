package btree;

import chainexception.ChainException;

public class DeleteFileEntryException extends ChainException {
    public DeleteFileEntryException() {
        super();
    }

    public DeleteFileEntryException(String s) {
        super(null, s);
    }

    public DeleteFileEntryException(Exception e, String s) {
        super(e, s);
    }

}
