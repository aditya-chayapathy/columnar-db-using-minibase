package catalog;

import chainexception.ChainException;

public class AttrCatalogException extends ChainException {

    public AttrCatalogException(Exception err, String name) {
        super(err, name);
    }
}

