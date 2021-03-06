package simpledb;
import java.util.*;
import java.io.*;
/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    private HeapPageId pid;
    private TupleDesc td;     //Tuple descriptor for this Dbfile
    private byte header[];    //Bitmap    #slotId, each bit of #slotId in bitmap corresponds to a status of tuple of #slotId
                              // status = ?empty:used 0:1
                              // R0 status | R1 status | R2 status | R3 status |
    private Tuple tuples[];   //Records in this page
    private int numSlots;     //Number of record slots
    private boolean isDirty;
    private TransactionId dirtyTid;

    byte[] oldData;
    private final Byte oldDataLock = new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        int bitsPerTupleIncludingHeader = td.getSize() * 8 + 1;
        int tuplesPerPage = (BufferPool.getPageSize()*8) / bitsPerTupleIncludingHeader; //round down
        return tuplesPerPage;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        // Bug fixed, turn 8 --> 8.0 will turn header's size from 42 to 43 TODO: Why?
        // 8 tuples occupy 8 bits of bitmap, which is 1 byte
        // e.g. 33 tuples will occupy 5 bytes, with only having 1 effective bit in the last byte
        // 1 byte size = 1 array size
        int headerSize = (int) Math.ceil(getNumTuples() / 8.0);
        return headerSize;
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
    //throw new UnsupportedOperationException("implement this");
        HeapPageId pageId = this.pid;
        return pageId;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        if (!rid.getPageId().equals(this.pid)) throw new DbException("Tuple does not belong to page");
        int slotId = rid.tupleno();
        if (!isSlotUsed(slotId)) throw new DbException("Tuple slot is empty");
        tuples[slotId] = null;
        markSlotUsed(slotId, false);
    }
    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!td.equals(t.getTupleDesc())) throw new DbException("Tuple descriptor mismatch");
        if (getNumEmptySlots() == 0) throw new DbException("Page is full");
        int slotId = -1;
        // where the empty slot is
        for (int i = 0; i < numSlots; i ++) {
            if (!isSlotUsed(i)) {
                slotId = i;
                break;
            }
        }
        if (slotId == -1) throw new DbException("Cant find empty slot when having unused slot");
        t.setRecordId(new RecordId(this.pid, slotId));
        tuples[slotId] = t;
        markSlotUsed(slotId, true);

    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	    // not necessary for lab1
        if (dirty) {
            this.dirtyTid = tid;
        } else {
            this.dirtyTid = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	    // Not necessary for lab1
        TransactionId tid = this.dirtyTid;
        return tid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int count = 0;
        for (int i = 0; i < getNumTuples(); i++) {
            if (!isSlotUsed(i)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     * Bug fixed
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        int headerbit = i % 8;      // offset of the status bit of tuple i
        int headerbyte = (i - headerbit) / 8;  //
        return (header[headerbyte] & (1 << headerbit)) != 0;

//        // FIXME: ScanSmall debug
//        // FIXME: ArrayIndexOutofBound when having random table with 3 columns 0 row
//        // FIXME: @parse i / 8.0, which should < header.length = 42
//        // FIXME: Bug situation 336/8 = 42 --> ArrayIndexOutOfBound
//        //if (i / 8 > header.length) return false;
//
//        int bits = header[ (int) (i / 8.0) ];
//        // Check if corresponding bit is set
//        return (bits & (1 << ( i % 8 )) ) > 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        if (value) {
            this.header[i / 8] |= (1 << (i % 8));
        } else {
            this.header[i / 8] &= ~(1 << (i % 8));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new PageTupleIterator();
    }
    private class PageTupleIterator implements Iterator<Tuple> {
        private Tuple[] tupleList;     //tuples which have been set
        private int size;
        private int index;

        public PageTupleIterator() {
            this.size = getNumTuples() - getNumEmptySlots();
            this.tupleList= new Tuple[size];
            for (int i = 0, j = 0; i < numSlots; i++) {
                if (isSlotUsed(i)) {
                    tupleList[j] = tuples[i];
                    j++;
                }
            }
            this.index = 0;
        }

        @Override
        public boolean hasNext(){
            if (index < size ) {
                return true;
            } else {
                return false;
            }
        }
        @Override
        public Tuple next() {
            Tuple returnValue = tupleList[index];
            index++;
            return returnValue;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}

