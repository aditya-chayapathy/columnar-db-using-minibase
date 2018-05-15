package diskmgr;

import chainexception.ChainException;


public class FileEntryNotFoundException extends ChainException {

    public FileEntryNotFoundException(Exception e, String name) {
        super(e, name);
    }


}




