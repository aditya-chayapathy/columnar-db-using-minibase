package catalog;

import chainexception.ChainException;

public class Catalogattrnotfound extends ChainException {

    public Catalogattrnotfound(Exception err, String name) {
        super(err, name);
    }
}

