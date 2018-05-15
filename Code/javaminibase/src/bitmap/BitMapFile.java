package bitmap;

import btree.*;
import columnar.Columnarfile;
import columnar.ValueClass;
import columnar.ValueInt;
import columnar.ValueString;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import heap.HFBufMgrException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BitMapFile implements GlobalConst {
    private String fileName;
    private BitMapHeaderPage headerPage;
    private PageId headerPageId;
    private String columnarFileName;
    private Integer columnNumber;
    private AttrType attrType;
    private ValueClass value;

    /***
     * Getter for columnNumber
     * @return
     */
    public Integer getColumnNumber() {
        return columnNumber;
    }

    /***
     * Setter got cokumnNumber
     * @param columnNumber
     */
    public void setColumnNumber(Integer columnNumber) {
        this.columnNumber = columnNumber;
    }

    /***
     * Getter for value
     * @return
     */
    public ValueClass getValue() {
        return value;
    }

    /***
     * Setter for value
     * @param value
     */
    public void setValue(ValueClass value) {
        this.value = value;
    }

    /***
     * getter fine name
     * @return
     */
    public String getFileName() {
        return fileName;
    }

    /***
     * setter for file name
     * @param fileName
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /***
     * getter for header page
     * @return
     */
    public BitMapHeaderPage getHeaderPage() {
        return headerPage;
    }

    /***
     * setter for header page
     * @param headerPage
     */
    public void setHeaderPage(BitMapHeaderPage headerPage) {
        this.headerPage = headerPage;
    }

    /***
     * opens an existing bitmap index file
     * @param fileName
     * @throws Exception
     */
    public BitMapFile(String fileName) throws Exception {
        this.fileName = fileName;
        headerPageId = get_file_entry(fileName);
        if (headerPageId == null) {
            throw new Exception("This index file (" + fileName + ") doesn't exist");
        }
        headerPage = new BitMapHeaderPage(headerPageId);

        columnarFileName = headerPage.getColumnarFileName();
        columnNumber = headerPage.getColumnNumber();
        attrType = headerPage.getAttrType();
        if (attrType.attrType == AttrType.attrString) {
            value = new ValueString(headerPage.getValue());
        } else {
            value = new ValueInt(Integer.parseInt(headerPage.getValue()));
        }
    }

    /***
     * creates a new bitmap file
     * @param filename
     * @param columnarFile
     * @param columnNo
     * @param value
     * @throws Exception
     */
    public BitMapFile(String filename, Columnarfile columnarFile, Integer columnNo, ValueClass value) throws Exception {
        headerPageId = get_file_entry(filename);
        if (headerPageId == null) //file does not exist
        {
            headerPage = new BitMapHeaderPage();
            headerPageId = headerPage.getPageId();
            add_file_entry(filename, headerPageId);
            headerPage.set_rootId(new PageId(INVALID_PAGE));
            headerPage.setColumnarFileName(columnarFile.getColumnarFileName());
            headerPage.setColumnNumber(columnNo);
            if (value instanceof ValueInt) {
                headerPage.setValue(value.getValue().toString());
                headerPage.setAttrType(new AttrType(AttrType.attrInteger));
            } else {
                headerPage.setValue(value.getValue().toString());
                headerPage.setAttrType(new AttrType(AttrType.attrString));
            }
        } else {
            headerPage = new BitMapHeaderPage(headerPageId);
        }
    }

    /***
     * closes the bitmap file
     * @throws Exception
     */
    public void close() throws Exception {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
    }

    /***
     * closes the bitmap file
     * @throws Exception
     */
    public void scanClose() throws Exception {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, false);
            headerPage = null;
        }
    }

    /***
     * delete all the bitmap related data
     * @throws Exception
     */
    public void destroyBitMapFile() throws Exception {
        if (headerPage != null) {
            PageId pgId = headerPage.get_rootId();
            BMPage bmPage = new BMPage();
            while (pgId.pid != INVALID_PAGE) {
                Page page = pinPage(pgId);
                bmPage.openBMpage(page);
                pgId = bmPage.getNextPage();
                unpinPage(pgId);
                freePage(pgId);
            }
            unpinPage(headerPageId);
            freePage(headerPageId);
            delete_file_entry(fileName);
            headerPage = null;
        }
    }

    /***
     * Update the bitmap value at the position specified.
     * @param set
     * @param position
     * @throws Exception
     */
    private void setValueAtPosition(boolean set, int position) throws Exception {
        List<PageId> pinnedPages = new ArrayList<>();
        if (headerPage == null) {
            throw new Exception("Bitmap header page is null");
        }
        if (headerPage.get_rootId().pid != INVALID_PAGE) {
            int pageCounter = 1;
            while (position >= BMPage.NUM_POSITIONS_IN_A_PAGE) {
                pageCounter++;
                position -= BMPage.NUM_POSITIONS_IN_A_PAGE;
            }
            PageId bmPageId = headerPage.get_rootId();
            Page page = pinPage(bmPageId);
            pinnedPages.add(bmPageId);
            BMPage bmPage = new BMPage(page);
            for (int i = 1; i < pageCounter; i++) {
                bmPageId = bmPage.getNextPage();
                if (bmPageId.pid == BMPage.INVALID_PAGE) {
                    PageId newPageId = getNewBMPage(bmPage.getCurPage());
                    pinnedPages.add(newPageId);
                    bmPage.setNextPage(newPageId);
                    bmPageId = newPageId;
                }
                page = pinPage(bmPageId);
                bmPage = new BMPage(page);
            }
            byte[] currData = bmPage.getBMpageArray();
            int bytoPos = position/8;
            int bitPos = position%8;
            if(set)
                currData[bytoPos] |= (1<<bitPos);
            else
                currData[bytoPos] &= ~(1<<bitPos);
            bmPage.writeBMPageArray(currData);
            if (bmPage.getCounter() < position + 1) {
                bmPage.updateCounter((short) (position + 1));
            }
        } else {
            PageId newPageId = getNewBMPage(headerPageId);
            pinnedPages.add(newPageId);
            headerPage.set_rootId(newPageId);
            setValueAtPosition(set, position);
        }
        for (PageId pinnedPage : pinnedPages) {
            unpinPage(pinnedPage, true);
        }
    }

    /***
     * delete bit at the posiition and re-organise the bitmap
     * @param position
     * @return
     * @throws Exception
     */
    public Boolean fullDelete(int position) throws Exception {
        if (headerPage == null) {
            throw new Exception("Bitmap header page is null");
        }
        List<PageId> pinnedPages = new ArrayList<>();
        if (headerPage.get_rootId().pid != INVALID_PAGE) {
            int pageCounter = 1;
            while (position >= BMPage.NUM_POSITIONS_IN_A_PAGE) {
                pageCounter++;
                position -= BMPage.NUM_POSITIONS_IN_A_PAGE;
            }
            PageId bmPageId = headerPage.get_rootId();
            Page page = pinPage(bmPageId);
            pinnedPages.add(bmPageId);
            BMPage bmPage = new BMPage(page);
            for (int i = 1; i < pageCounter; i++) {
                bmPageId = bmPage.getNextPage();
                page = pinPage(bmPageId);
                bmPage = new BMPage(page);
            }
            BitSet bitSet = BitSet.valueOf(bmPage.getBMpageArray());
            boolean lastBit = _fullDelete(bmPage.getNextPage());
            while (position < bitSet.length()) {
                int position1 = bitSet.nextSetBit(position);
                bitSet.clear(position1);
                bitSet.clear(position, position1);
                bitSet.set(position1-1);
                position = position1;
            }
            if(lastBit)
                bitSet.set(bitSet.length()-1);
            bmPage.writeBMPageArray(bitSet.toByteArray());
            for (PageId pinnedPage : pinnedPages) {
                unpinPage(pinnedPage, true);
            }
        }
        return Boolean.TRUE;
    }

    /***
     * delete bit at the posiition and re-organise the bitmap
     * @param pageId
     * @return
     */
    private boolean _fullDelete(PageId pageId){
        if(pageId.pid == INVALID_PAGE)
            return false;

        try {
            Page page = pinPage(pageId);
            BMPage bmPage = new BMPage(page);
            BitSet bitSet = BitSet.valueOf(bmPage.getBMpageArray());
            boolean firstBit = bitSet.get(0);
            bitSet = bitSet.get(1, bitSet.length());
            boolean lastBit = _fullDelete(bmPage.getNextPage());
            if(lastBit)
                bitSet.set(bitSet.length()-1);
            bmPage.writeBMPageArray(bitSet.toByteArray());
            return firstBit;
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }

    }

    /***
     * mark postion with value 0
     * @param position
     * @return
     * @throws Exception
     */
    public Boolean delete(int position) throws Exception {
        setValueAtPosition(false, position);
        return Boolean.TRUE;
    }

    /***
     * mark postion with value 1
     * @param position
     * @return
     * @throws Exception
     */
    public Boolean insert(int position) throws Exception {
        setValueAtPosition(true, position);
        return Boolean.TRUE;
    }

    /***
     * Allocate new BMPage to the doubly linked list
     * @param prevPageId
     * @return
     * @throws Exception
     */
    private PageId getNewBMPage(PageId prevPageId) throws Exception {
        Page apage = new Page();
        PageId pageId = newPage(apage, 1);
        BMPage bmPage = new BMPage();
        bmPage.init(pageId, apage);
        bmPage.setPrevPage(prevPageId);

        return pageId;
    }

    /***
     * Get file entry from DB
     * @param filename
     * @return
     * @throws GetFileEntryException
     */
    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    /***
     * Add file entry to the DB
     * @param fileName
     * @param pageno
     * @throws AddFileEntryException
     */
    private void add_file_entry(String fileName, PageId pageno)
            throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    /***
     * delete file entry from DB
     * @param filename
     * @throws DeleteFileEntryException
     */
    private void delete_file_entry(String filename)
            throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
    }

    /**
     *
     * @return scan object
     * @throws Exception
     */
    public BitmapFileScan new_scan() throws Exception {
        return new BitmapFileScan(this);
    }

    /***
     * Unpin pagefrom buffer
     * @param pageno
     * @throws UnpinPageException
     */
    private void unpinPage(PageId pageno)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    /***
     * Unpin pagefrom buffer
     * @param pageno
     * @param dirty
     * @throws UnpinPageException
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    /***
     * free page passed as input
     * @param pageno
     * @throws FreePageException
     */
    private void freePage(PageId pageno)
            throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    /***
     * pin page passed as input
     * @param pageno
     * @return
     * @throws PinPageException
     */
    private Page pinPage(PageId pageno)
            throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    /***
     * Allocate new page
     * @param page
     * @param num
     * @return
     * @throws HFBufMgrException
     */
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

    public PageId getHeaderPageId() {
        return headerPageId;
    }
}
