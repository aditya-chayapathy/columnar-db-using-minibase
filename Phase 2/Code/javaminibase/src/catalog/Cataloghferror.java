package catalog;

import chainexception.ChainException;

public class Cataloghferror extends ChainException {

    public Cataloghferror() {
        super();
    }

    public Cataloghferror(Exception err, String name) {
        super(err, name);
    }
}

