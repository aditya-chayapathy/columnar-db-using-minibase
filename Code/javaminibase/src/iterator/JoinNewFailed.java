package iterator;

import chainexception.ChainException;

public class JoinNewFailed extends ChainException {
    public JoinNewFailed(String s) {
        super(null, s);
    }

    public JoinNewFailed(Exception prev, String s) {
        super(prev, s);
    }
}
