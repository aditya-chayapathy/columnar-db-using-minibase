package bitmap;

import btree.ConstructPageException;
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

import java.io.IOException;

public class BitMapHeaderPage extends HFPage {

    public static final int DPFIXED = 4 * 2 + 3 * 4;
    public static final int COLUMN_NUMBER_SIZE = 2;
    public static final int ATTR_TYPE_SIZE = 2;
    public static final int COLUMNNAR_FILE_NAME_SIZE = 200;
    public static final int VALUE_SIZE = 400;

    public static final int COLUMN_NUMBER_POSITION = DPFIXED;
    public static final int ATTR_TYPE_POSITION = COLUMN_NUMBER_POSITION + COLUMN_NUMBER_SIZE;
    public static final int COLUMNNAR_FILE_NAME_POSITION = ATTR_TYPE_POSITION + ATTR_TYPE_SIZE;
    public static final int VALUE_POSITION = COLUMNNAR_FILE_NAME_POSITION + COLUMNNAR_FILE_NAME_SIZE;

    /***
     * Pretty print the header page contents
     * @throws Exception
     */
    public void dumpHeaderPage() throws Exception {
        System.out.println("Dump Header Page");
        System.out.println("Colmnnar File Name= " + getColumnarFileName());
        System.out.println("Column Number= " + getColumnNumber());
        System.out.println("Attribute Type= " + getAttrType());
        System.out.println("Value= " + getValue());
        System.out.println("First BMPage PageId= " + get_rootId());
        System.out.println("Header Page Id= " + getPageId());
    }

    /***
     * Setter for column number
     * @param columnNumber
     * @throws Exception
     */
    public void setColumnNumber(int columnNumber) throws Exception {
        Convert.setShortValue((short) columnNumber, COLUMN_NUMBER_POSITION, data);
    }

    /***
     * Setter for attribute type
     * @param attrType
     * @throws Exception
     */
    public void setAttrType(AttrType attrType) throws Exception {
        Convert.setShortValue((short) attrType.attrType, ATTR_TYPE_POSITION, data);
    }

    /***
     * Setter for columnnar file name
     * @param columnnarFileName
     * @throws Exception
     */
    public void setColumnarFileName(String columnnarFileName) throws Exception {
        Convert.setStrValue(columnnarFileName, COLUMNNAR_FILE_NAME_POSITION, data);
    }

    /***
     * Setter for value
     * @param value
     * @throws Exception
     */
    public void setValue(String value) throws Exception {
        Convert.setStrValue(value, VALUE_POSITION, data);
    }

    /***
     * getter for column number
     * @return
     * @throws Exception
     */
    public Integer getColumnNumber() throws Exception {
        short val = Convert.getShortValue(COLUMN_NUMBER_POSITION, data);
        return (int) val;
    }

    /***
     * getter for attribute type
     * @return
     * @throws Exception
     */
    public AttrType getAttrType() throws Exception {
        short val = Convert.getShortValue(ATTR_TYPE_POSITION, data);
        return new AttrType(val);
    }

    /***
     * getter for columnar file name
     * @return
     * @throws Exception
     */
    public String getColumnarFileName() throws Exception {
        String val = Convert.getStrValue(COLUMNNAR_FILE_NAME_POSITION, data, COLUMNNAR_FILE_NAME_SIZE);
        return val.trim();
    }

    /***
     * getter for value
     * @return
     * @throws Exception
     */
    public String getValue() throws Exception {
        String val = Convert.getStrValue(VALUE_POSITION, data, VALUE_SIZE);
        return val.trim();
    }

    /***
     * opens the header file for the page id passed as input
     * @param pageno
     * @throws ConstructPageException
     */
    public BitMapHeaderPage(PageId pageno)
            throws ConstructPageException {
        super();
        try {

            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }

    /***
     * open the bitmap header page passed as input
     * @param page
     */
    public BitMapHeaderPage(Page page) {
        super(page);
    }

    /***
     * Default constructor. Creates a new header page.
     * @throws ConstructPageException
     */
    public BitMapHeaderPage() throws ConstructPageException {
        super();
        try {
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
            if (pageId == null)
                throw new ConstructPageException(null, "new page failed");
            this.init(pageId, apage);

        } catch (Exception e) {
            throw new ConstructPageException(e, "construct header page failed");
        }
    }

    /***
     * getter for page id
     * @return
     * @throws IOException
     */
    public PageId getPageId() throws IOException {
        return getCurPage();
    }

    /***
     * setter for page id
     * @param pageno
     * @throws IOException
     */
    void setPageId(PageId pageno)
            throws IOException {
        setCurPage(pageno);
    }

    /***
     * getter for root id
     * @return
     * @throws IOException
     */
    public PageId get_rootId()
            throws IOException {
        return getNextPage();
    }

    /***
     * setter for rootid
     * @param rootID
     * @throws IOException
     */
    void set_rootId(PageId rootID)
            throws IOException {
        setNextPage(rootID);
    }
}
