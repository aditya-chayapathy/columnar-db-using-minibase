package global;

import heap.Tuple;

public interface IScan {

    Tuple get_next() throws Exception;

    boolean delete_next() throws Exception;

    void close() throws Exception;
}
