package catalog;

import chainexception.ChainException;

public class Catalogmissparam extends ChainException {

    public Catalogmissparam(Exception err, String name) {
        super(err, name);
    }
}

