package diskmgr;

import chainexception.ChainException;


public class DiskMgrException extends ChainException {

    public DiskMgrException(Exception e, String name)

    {
        super(e, name);
    }


}




