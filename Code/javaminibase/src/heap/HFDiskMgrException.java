/* File hferr.java  */

package heap;

import chainexception.ChainException;

public class HFDiskMgrException extends ChainException {


    public HFDiskMgrException() {
        super();

    }

    public HFDiskMgrException(Exception ex, String name) {
        super(ex, name);
    }


}
