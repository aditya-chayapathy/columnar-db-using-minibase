package catalog;

import chainexception.ChainException;

public class Catalogattrexists extends ChainException {

    public Catalogattrexists(Exception err, String name) {
        super(err, name);
    }
}

