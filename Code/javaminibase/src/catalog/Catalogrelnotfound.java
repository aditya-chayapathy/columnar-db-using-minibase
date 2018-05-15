package catalog;

import chainexception.ChainException;

public class Catalogrelnotfound extends ChainException {

    public Catalogrelnotfound(Exception err, String name) {
        super(err, name);
    }
}

