package catalog;

import chainexception.ChainException;

public class Catalogrelexists extends ChainException {

    public Catalogrelexists(Exception err, String name) {
        super(err, name);
    }
}

