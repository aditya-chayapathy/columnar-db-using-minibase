package catalog;

import chainexception.ChainException;

public class Catalogindexnotfound extends ChainException {

    public Catalogindexnotfound() {
        super();
    }

    public Catalogindexnotfound(Exception err, String name) {
        super(err, name);
    }
}

