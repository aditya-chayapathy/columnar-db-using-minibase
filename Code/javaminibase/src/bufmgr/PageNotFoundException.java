package bufmgr;

import chainexception.ChainException;


public class PageNotFoundException extends ChainException {

    public PageNotFoundException(Exception e, String name) {
        super(e, name);
    }


}




