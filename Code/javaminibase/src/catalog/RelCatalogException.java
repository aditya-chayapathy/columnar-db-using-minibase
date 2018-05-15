package catalog;

import chainexception.ChainException;

public class RelCatalogException extends ChainException {

    public RelCatalogException(Exception err, String name) {
        super(err, name);
    }
}

