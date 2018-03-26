package bufmgr;

import chainexception.ChainException;

public class BufferPoolExceededException extends ChainException {

    public BufferPoolExceededException(Exception e, String name) {
        super(e, name);
    }

}
