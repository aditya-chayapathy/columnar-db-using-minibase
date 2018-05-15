package catalog;

import chainexception.ChainException;

public class Catalogindexexists extends ChainException {

    public Catalogindexexists() {
        super();
    }

    public Catalogindexexists(Exception err, String name) {
        super(err, name);
    }
}

