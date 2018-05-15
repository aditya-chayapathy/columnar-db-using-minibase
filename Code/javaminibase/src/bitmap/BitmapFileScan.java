package bitmap;

import btree.PinPageException;
import diskmgr.Page;
import global.*;
import heap.HFBufMgrException;
import index.IndexException;
import iterator.JoinsException;
import iterator.SortException;

import java.io.IOException;
import java.util.BitSet;

import static global.GlobalConst.INVALID_PAGE;

public class BitmapFileScan {

    BitMapFile file;
    private PageId currentPageId;
    private BitSet bitMaps;
    private BMPage currentBMPage;
    public int counter;
    private int scanCounter = 0;

    public BitmapFileScan(BitMapFile f) throws Exception {
        file = f;
        currentPageId = file.getHeaderPage().get_rootId();
        //value = bmIndFile.getHeaderPage().getValue();
        currentBMPage = new BMPage(pinPage(currentPageId));
        counter = currentBMPage.getCounter();
        bitMaps = BitSet.valueOf(currentBMPage.getBMpageArray());
    }

    private Page pinPage(PageId pageno) throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    /**
     * short cut to access the unpinPage function in bufmgr package.
     *
     * @see bufmgr.unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }
    }

    public int get_next(){
        try {

            if (scanCounter > counter) {
                PageId nextPage = currentBMPage.getNextPage();
                unpinPage(currentPageId, false);
                if (nextPage.pid != INVALID_PAGE) {
                    currentPageId.copyPageId(nextPage);
                    currentBMPage = new BMPage(pinPage(currentPageId));
                    counter = currentBMPage.getCounter();
                    bitMaps = BitSet.valueOf(currentBMPage.getBMpageArray());
                } else {
                    return -1;
                }
            }
            while (scanCounter <= counter) {
                if (bitMaps.get(scanCounter)) {
                    int position = scanCounter;
                    scanCounter++;
                    return position;
                } else {
                    scanCounter++;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return -1;
    }

    public BitSet get_next_bitmap() {
        try {
            if (bitMaps != null) {
                BitSet currentBitMap = (BitSet) bitMaps.clone();
                PageId nextPage = currentBMPage.getNextPage();
                unpinPage(currentPageId, false);
                if (nextPage.pid != INVALID_PAGE) {
                    currentPageId.copyPageId(nextPage);
                    currentBMPage = new BMPage(pinPage(currentPageId));
                    counter = currentBMPage.getCounter();
                    bitMaps = BitSet.valueOf(currentBMPage.getBMpageArray());
                } else {
                    bitMaps = null;
                    currentPageId.copyPageId(nextPage);
                }
                return currentBitMap;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close() throws Exception {
        file.scanClose();
        if(currentPageId != null && currentPageId.pid != INVALID_PAGE)
            unpinPage(currentPageId, false);
    }
}
