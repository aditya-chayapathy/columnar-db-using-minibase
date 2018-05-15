package heap;

import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

import java.io.IOException;

public class RunFile implements heap.Filetype, GlobalConst {


    private static int tempfilecount = 0;
    PageId _firstDataPageId;   // page number of first page
    PageId _currentDataPageId;
    HFPage currentDataPage;
    int _ftype;
    private boolean _file_deleted;
    private String _fileName;
    private RID currRec = null;


    /**
     * Initialize.  A null name produces a temporary heapfile which will be
     * deleted by the destructor.  If the name already denotes a file, the
     * file is opened; otherwise, a new empty file is created.
     *
     * @throws HFException        heapfile exception
     * @throws HFBufMgrException  exception thrown from bufmgr layer
     * @throws HFDiskMgrException exception thrown from diskmgr layer
     * @throws IOException        I/O errors
     */
    public RunFile()
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException

    {
        // Give us a prayer of destructing cleanly if construction fails.
        _file_deleted = true;
        _fileName = null;
        // If the name is NULL, allocate a temporary name
        // and no logging is required.
        _fileName = "runFile";
        String useId = new String("user.name");
        String userAccName;
        userAccName = System.getProperty(useId);
        _fileName = _fileName + userAccName;

        String filenum = Integer.toString(tempfilecount);
        _fileName = _fileName + filenum;
        _ftype = TEMP;
        tempfilecount++;

        Page apage = new Page();
        _firstDataPageId = null;
        // file doesn't exist. First create it.
        _firstDataPageId = newPage(apage, 1);
        // check error
        if (_firstDataPageId == null)
            throw new HFException(null, "can't new page");

        add_file_entry(_fileName, _firstDataPageId);
        // check error(new exception: Could not add file entry

        HFPage firstDataPage = new HFPage();
        firstDataPage.init(_firstDataPageId, apage);
        PageId pageId = new PageId(INVALID_PAGE);
        //_currentDataPageId = new PageId(_firstDataPageId.pid);
        firstDataPage.setNextPage(pageId);
        firstDataPage.setPrevPage(pageId);
        unpinPage(_firstDataPageId, true /*dirty*/);
        _file_deleted = false;
    }

    /* get a new datapage from the buffer manager and initialize dpinfo
       @param dpinfop the information in the new HFPage
    */
    private void _newDatapage(PageId pid, PageId prevPage)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        Page apage = new Page();
        PageId pageId = new PageId();
        pageId = newPage(apage, 1);
        if (pageId == null)
            throw new HFException(null, "can't new pae");
        // initialize internal values of the new page:
        HFPage hfpage = new HFPage();
        hfpage.init(pageId, apage);
        pid.pid = pageId.pid;
        hfpage.setPrevPage(prevPage);
        hfpage.setNextPage(new PageId(INVALID_PAGE));
        unpinPage(pageId, true);
    } // end of _newDatapage

    /**
     * Insert record into file, return its Rid.
     *
     * @param recPtr pointer of the record
     * @return the rid of the record
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws SpaceNotAvailableException no space left
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws IOException                I/O errors
     */
    public RID insertRecord(byte[] recPtr)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        int recLen = recPtr.length;
        PageId currentDataPageId = new PageId(_firstDataPageId.pid);
        HFPage currentDataPage = new HFPage();
        pinPage(currentDataPageId, currentDataPage, false/*Rdisk*/);

        while (true) {
            if (currentDataPage.available_space() >= recLen){
                break;
            }
            PageId nextPageId = currentDataPage.getNextPage();
            HFPage nextdataPage = new HFPage();
            boolean dirty = false;
            if(nextPageId.pid == INVALID_PAGE){
                _newDatapage(nextPageId, currentDataPageId);
                currentDataPage.setNextPage(nextPageId);
                dirty = true;
            }
            unpinPage(currentDataPageId, dirty);
            pinPage(nextPageId,nextdataPage,false);

            currentDataPageId = new PageId(nextPageId.pid);
            currentDataPage = nextdataPage;
        }

        RID rid;
        rid = currentDataPage.insertRecord(recPtr);

        unpinPage(currentDataPageId, true /* = DIRTY */);
        return rid;
    }

    public void initiateSequentialInsert() throws HFBufMgrException, IOException {
        _currentDataPageId = _firstDataPageId;
        currentDataPage = new HFPage();
        pinPage(_currentDataPageId, currentDataPage, false);
    }

    public void insertNext(byte[] recPtr) throws HFBufMgrException, IOException, InvalidSlotNumberException, HFException, HFDiskMgrException {

        int recLen = recPtr.length;
        while (true) {
            if (currentDataPage.available_space() >= recLen){
                break;
            }
            PageId nextPageId = currentDataPage.getNextPage();
            HFPage nextdataPage = new HFPage();
            boolean dirty = false;
            if(nextPageId.pid == INVALID_PAGE){
                _newDatapage(nextPageId, _currentDataPageId);
                currentDataPage.setNextPage(nextPageId);
                dirty = true;
            }
            unpinPage(_currentDataPageId, dirty);
            pinPage(nextPageId,nextdataPage,false);

            _currentDataPageId = new PageId(nextPageId.pid);
            currentDataPage = nextdataPage;
        }
        currentDataPage.insertRecord(recPtr);
    }

    public void finishSequentialInsert() throws HFBufMgrException {
        unpinPage(_currentDataPageId, false);
        _currentDataPageId = null;
    }

    public void initiateScan() throws HFBufMgrException, IOException {
        _currentDataPageId = _firstDataPageId;
        currentDataPage = new HFPage();
        pinPage(_currentDataPageId, currentDataPage, false);
        currRec = currentDataPage.firstRecord();
    }

    public Tuple getNext() throws HFBufMgrException, IOException, InvalidSlotNumberException {

        if(currRec == null)
            return null;
        Tuple t = currentDataPage.getRecord(currRec);
        currRec = currentDataPage.nextRecord(currRec);
        if(currRec == null){
            PageId nextPageId = currentDataPage.getNextPage();
            if(nextPageId.pid == INVALID_PAGE) {
                currRec = null;
                return t;
            }
            unpinPage(_currentDataPageId, false);
            pinPage(nextPageId, currentDataPage, false);
            _currentDataPageId = new PageId(nextPageId.pid);
            currRec = currentDataPage.firstRecord();
        }
        return t;
    }

    public void setPrev() throws HFBufMgrException, IOException, InvalidSlotNumberException {

        if(currRec == null)
            return;
        currRec = currentDataPage.prevRecord(currRec);
        if(currRec == null){
            PageId prevPageId = currentDataPage.getPrevPage();
            unpinPage(_currentDataPageId, false);
            pinPage(prevPageId, currentDataPage, false);
            _currentDataPageId = new PageId(prevPageId.pid);
            currRec = currentDataPage.lastRecord();
        }
    }

    public void finishScan() throws HFBufMgrException {
        unpinPage(_currentDataPageId, false);
        _currentDataPageId = null;
    }


    /**
     * Delete the file from the database.
     *
     * @throws InvalidSlotNumberException  invalid slot number
     * @throws InvalidTupleSizeException   invalid tuple size
     * @throws FileAlreadyDeletedException file is deleted already
     * @throws HFBufMgrException           exception thrown from bufmgr layer
     * @throws HFDiskMgrException          exception thrown from diskmgr layer
     * @throws IOException                 I/O errors
     */
    public void deleteFile()
            throws InvalidSlotNumberException,
            FileAlreadyDeletedException,
            InvalidTupleSizeException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        if (_file_deleted)
            throw new FileAlreadyDeletedException(null, "file alread deleted");


        // Mark the deleted flag (even if it doesn't get all the way done).
        _file_deleted = true;

        // Deallocate all data pages
        PageId currentDataPageId = new PageId();
        currentDataPageId.pid = _firstDataPageId.pid;
        //PageId nextDataPageId = new PageId();
        //nextDataPageId.pid = 0;
        HFPage currentDataPage = new HFPage();

        while (currentDataPageId.pid != INVALID_PAGE) {
            pinPage(currentDataPageId, currentDataPage, false);
            PageId nextDataPageId = currentDataPage.getNextPage();
            freePage(currentDataPageId);
            currentDataPageId = new PageId(nextDataPageId.pid);
        }
        delete_file_entry(_fileName);
    }



    /**
     * short cut to access the pinPage function in bufmgr package.
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

    private void freePage(PageId pageno)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: freePage() failed");
        }

    } // end of freePage

    private PageId newPage(Page page, int num)
            throws HFBufMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page, num);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
        }

        return tmpId;

    } // end of newPage

    private PageId get_file_entry(String filename)
            throws HFDiskMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: get_file_entry() failed");
        }

        return tmpId;

    } // end of get_file_entry

    private void add_file_entry(String filename, PageId pageno)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: add_file_entry() failed");
        }

    } // end of add_file_entry

    private void delete_file_entry(String filename)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: delete_file_entry() failed");
        }

    } // end of delete_file_entry

}
