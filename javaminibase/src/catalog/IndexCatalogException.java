package catalog;

import chainexception.ChainException;

public class IndexCatalogException extends ChainException {

    public IndexCatalogException(Exception err, String name) {
        super(err, name);
    }
}

