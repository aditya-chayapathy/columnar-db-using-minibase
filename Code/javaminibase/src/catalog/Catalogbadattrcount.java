package catalog;

import chainexception.ChainException;

public class Catalogbadattrcount extends ChainException {

    public Catalogbadattrcount(Exception err, String name) {
        super(err, name);
    }
}

