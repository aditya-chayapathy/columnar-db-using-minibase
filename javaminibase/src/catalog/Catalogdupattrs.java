package catalog;

import chainexception.ChainException;

public class Catalogdupattrs extends ChainException {

    public Catalogdupattrs(Exception err, String name) {
        super(err, name);
    }
}

