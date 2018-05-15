package bitmap;

import diskmgr.Page;
import global.Convert;
import global.GlobalConst;
import global.PageId;

import java.io.IOException;

public class BMPage extends Page implements GlobalConst {

    public static final int DPFIXED = 2 * 2 + 3 * 4;
    public static final int NUM_POSITIONS_IN_A_PAGE = (MAX_SPACE - DPFIXED)*8;

    public static final int COUNTER = 0;
    public static final int FREE_SPACE = 2;
    public static final int PREV_PAGE = 4;
    public static final int NEXT_PAGE = 8;
    public static final int CUR_PAGE = 12;


    private PageId curPage = new PageId();
    private short counter;
    private short freeSpace;
    private PageId prevPage = new PageId();
    private PageId nextPage = new PageId();

    /***
     * Default constructor
     */
    public BMPage() {
    }

    /***
     * Take a BMPage as input and loads the current page with it's contents
     * @param page
     */
    public BMPage(Page page) {
        data = page.getpage();
    }

    /***
     * Get available space present in the BMPage
     * @return
     * @throws IOException
     */
    public int available_space() throws IOException {
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        return freeSpace;
    }

    /***
     * Pretty print the contents of the dunp page
     * @throws IOException
     */
    public void dumpPage() throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        counter = Convert.getShortValue(COUNTER, data);
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);

        System.out.println("dumpPage");
        System.out.println("prevPage= " + prevPage.pid);
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("counter= " + counter);

        for (int i = 0; i < counter; i++) {
            Integer position = DPFIXED + i;
            Byte val = Convert.getByteValue(position, data);
            System.out.println("position=" + i + "  value=" + val);
        }
    }

    /***
     * Checks if the page is empty
     * @return
     * @throws IOException
     */
    public boolean empty() throws IOException {
        int availableSpace = available_space();
        if (availableSpace == MAX_SPACE - DPFIXED) {
            return true;
        }
        return false;
    }

    /***
     * Initializes the BMPage with the default metadata
     * @param pageNo
     * @param apage
     * @throws IOException
     */
    public void init(PageId pageNo, Page apage) throws IOException {
        data = apage.getpage();

        counter = (short) 0;
        Convert.setShortValue(counter, COUNTER, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

        freeSpace = (short) NUM_POSITIONS_IN_A_PAGE;    // amount of space available
        Convert.setShortValue(freeSpace, FREE_SPACE, data);

        for (int i = DPFIXED; i < MAX_SPACE; i++) {
            Convert.setByteValue((byte) 0, i, data);
        }
    }

    /***
     * Getter for counter
     * @return
     * @throws Exception
     */
    public Integer getCounter() throws Exception {
        return (int) Convert.getShortValue(COUNTER, data);
    }

    /***
     * Updates counter with the given value
     * @param value
     * @throws Exception
     */
    public void updateCounter(Short value) throws Exception {
        Convert.setShortValue(value, COUNTER, data);
        Convert.setShortValue((short) (NUM_POSITIONS_IN_A_PAGE - value), FREE_SPACE, data);
    }

    /***
     * Take a BMPage as input and loads the current page with it's contents
     * @param apage
     */
    public void openBMpage(Page apage) {
        data = apage.getpage();
    }

    /***
     * Getter for current page id
     * @return
     * @throws IOException
     */
    public PageId getCurPage()
            throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        return curPage;
    }

    /***
     * setter for the current page id
     * @param pageNo
     * @throws IOException
     */
    public void setCurPage(PageId pageNo)
            throws IOException {
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    }

    /***
     * getter for next page id
     * @return
     * @throws IOException
     */
    public PageId getNextPage()
            throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    /***
     * setter for next page id
     * @param pageNo
     * @throws IOException
     */
    public void setNextPage(PageId pageNo)
            throws IOException {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

    /***
     * getter for previous page id
     * @return
     * @throws IOException
     */
    public PageId getPrevPage()
            throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }

    /***
     * setter for previous page id
     * @param pageNo
     * @throws IOException
     */
    public void setPrevPage(PageId pageNo)
            throws IOException {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }

    /***
     * returns the bitmap array portion of the BMPage
     * @return
     * @throws Exception
     */
    public byte[] getBMpageArray() throws Exception {
        int numBytesInPage = NUM_POSITIONS_IN_A_PAGE /8;
        byte[] bitMapArray = new byte[numBytesInPage];
        for (int i = 0; i < numBytesInPage; i++) {
            bitMapArray[i] = Convert.getByteValue(DPFIXED + i, data);
        }
        return bitMapArray;
    }

    /***
     * Updates the bitmap portion of the BMPage
     * @param givenData
     * @throws Exception
     */
    void writeBMPageArray(byte[] givenData) throws Exception {
        int count = givenData.length;
        for (int i = 0; i < count; i++) {
            Convert.setByteValue(givenData[i], DPFIXED + i, data);
        }
    }
}
