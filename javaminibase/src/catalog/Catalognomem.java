package catalog;

import chainexception.ChainException;

public class Catalognomem extends ChainException {

    public Catalognomem(Exception err, String name) {
        super(err, name);
    }
}

