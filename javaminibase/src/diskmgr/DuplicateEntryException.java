package diskmgr;

import chainexception.ChainException;

public class DuplicateEntryException extends ChainException {

    public DuplicateEntryException(Exception e, String name) {
        super(e, name);
    }
}

