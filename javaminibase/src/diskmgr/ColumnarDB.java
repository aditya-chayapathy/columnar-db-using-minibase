package diskmgr;

import global.PageId;

import java.io.IOException;

/**
 * Created by dixith on 3/24/18.
 */
public class ColumnarDB extends DB {

    public  void read_page(PageId pageno, Page apage)
            throws InvalidPageNumberException,
            FileIOException,
            IOException {

        if((pageno.pid < 0)||(pageno.pid >= num_pages))
            throw new InvalidPageNumberException(null, "BAD_PAGE_NUMBER");

        // Seek to the correct page
        fp.seek((long)(pageno.pid *MINIBASE_PAGESIZE));

        // Read the appropriate number of bytes.
        byte [] buffer = apage.getpage();  //new byte[MINIBASE_PAGESIZE];
        try{
            fp.read(buffer);
            PCounter.readIncrement();
        }
        catch (IOException e) {
            throw new FileIOException(e, "DB file I/O error");
        }
    }

    public void write_page(PageId pageno, Page apage)
            throws InvalidPageNumberException,
            FileIOException,
            IOException {

        if((pageno.pid < 0)||(pageno.pid >= num_pages))
            throw new InvalidPageNumberException(null, "INVALID_PAGE_NUMBER");

        // Seek to the correct page
        fp.seek((long)(pageno.pid *MINIBASE_PAGESIZE));

        // Write the appropriate number of bytes.
        try{
            fp.write(apage.getpage());
            PCounter.writeIncrement();
        }
        catch (IOException e) {
            throw new FileIOException(e, "DB file I/O error");
        }

    }
}
