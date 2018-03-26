package bufmgr;

import chainexception.ChainException;

public class PageUnpinnedException extends ChainException {

    public PageUnpinnedException(Exception ex, String name) {
        super(ex, name);
    }


}




