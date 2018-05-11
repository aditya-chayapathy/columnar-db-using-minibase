package bufmgr;

import chainexception.ChainException;

public class InvalidBufferException extends ChainException {


    public InvalidBufferException(Exception e, String name) {
        super(e, name);
    }

}




