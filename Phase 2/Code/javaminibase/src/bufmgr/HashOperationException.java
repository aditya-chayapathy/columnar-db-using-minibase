package bufmgr;

import chainexception.ChainException;

public class HashOperationException extends ChainException {

    public HashOperationException(Exception e, String name) {
        super(e, name);
    }


}




