package heap;

import chainexception.ChainException;

public class HFBufMgrException extends ChainException {


    public HFBufMgrException() {
        super();

    }

    public HFBufMgrException(Exception ex, String name) {
        super(ex, name);
    }


}
