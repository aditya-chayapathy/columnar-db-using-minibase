package bufmgr;

import chainexception.ChainException;

public class InvalidFrameNumberException extends ChainException {


    public InvalidFrameNumberException(Exception e, String name) {
        super(e, name);
    }


}




