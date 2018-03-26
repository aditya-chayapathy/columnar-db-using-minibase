package catalog;

import chainexception.ChainException;

public class Catalogbadtype extends ChainException {

    public Catalogbadtype(Exception err, String name) {
        super(err, name);
    }
}

