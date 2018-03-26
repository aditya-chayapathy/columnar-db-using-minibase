package bufmgr;

import chainexception.ChainException;


public class HashEntryNotFoundException extends ChainException {

    public HashEntryNotFoundException(Exception ex, String name) {
        super(ex, name);
    }


}




