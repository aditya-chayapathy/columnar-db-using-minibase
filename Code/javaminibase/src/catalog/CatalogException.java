package catalog;

import chainexception.ChainException;

public class CatalogException extends ChainException {

    public CatalogException(Exception err, String name) {
        super(err, name);
    }
}

