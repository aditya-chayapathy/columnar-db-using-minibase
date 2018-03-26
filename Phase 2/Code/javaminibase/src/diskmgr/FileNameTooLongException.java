package diskmgr;

import chainexception.ChainException;


public class FileNameTooLongException extends ChainException {

    public FileNameTooLongException(Exception ex, String name) {
        super(ex, name);
    }

}




