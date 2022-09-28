package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File _file; // 文件
    private TupleDesc _td; // 对于Tuple的描述
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // code done
        _file = f;
        _td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // code done
        return _file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // code done
        // throw new UnsupportedOperationException("implement this");
        return _file.getAbsoluteFile().hashCode(); // 唯一标识这个HeapFile
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // code done
        // throw new UnsupportedOperationException("implement this");
        return _td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // code done
        int offset = pid.getPageNumber() * BufferPool.getPageSize(); // 计算出这个Page在文件中的偏移量，页面数目乘以每个页的大小
        byte[] data = new byte[BufferPool.getPageSize()]; // 读取的数据
        try {
            RandomAccessFile raf = new RandomAccessFile(_file, "r"); // 以只读的方式打开文件，随机读取文件
            raf.seek(offset); // 定位到偏移量
            raf.read(data); // 读取数据
            raf.close(); // 关闭文件
        } catch (IOException e) {
            e.printStackTrace();
        }
        Page page = null;
        try {
            page = new HeapPage((HeapPageId) pid, data); // 创建一个HeapPage
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return page; // 返回一个HeapPage
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        int pid = page.getId().getPageNumber(); // 获取PageId
        int offset = pid * BufferPool.getPageSize(); // 计算出这个Page在文件中的偏移量，页面数目乘以每个页的大小
        // 写入文件
        RandomAccessFile raf = new RandomAccessFile(_file, "rw"); // 以读写的方式打开文件，随机读写文件
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // code done
        // 这个Heap文件一共有多少页
        return (int) Math.ceil((_file.length() / BufferPool.getPageSize())); // 文件的大小除以每个页的大小，就是页的数目
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // code done
        // 插入
        // not necessary for lab1
        HeapPageId pid = null;
        for (int i = 0; i < numPages(); i++) {
            pid = new HeapPageId(getId(), i); // 从第一页开始遍历,注意，这个地方要注意一下getId()返回的是不是HeapFile的table id
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE); // 可读可写
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t); // 插入
                return Arrays.asList(page); // 返回一个List，修改了一个page
            }
        }
        // 如果没有空闲的slot，就新建一个page
        HeapPage page = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        // 在新建的page中插入tuple，然后把这个pageflush到磁盘上
        page.insertTuple(t);
        writePage(page);
        return Arrays.asList(page);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // code done
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<Page>(Arrays.asList(page));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // code done
        DbFileIterator it = new DbFileIterator() {
            private PageId _pid; // 当前的PageId，因为一个heapFile对应一个表，所以HeapPageId的tableId是固定的
            private Iterator<Tuple> _it = null; // 当前页的迭代器
            private TransactionId _tid = tid; // 事务ID

            @Override
            public void open() throws DbException, TransactionAbortedException {
                // 不能将整个表都读入内存，遇到大表会导致OOM
                // 所以需要一个迭代器，每次将一页放入内存
                _pid = new HeapPageId(getId(), 0); // 从第一页开始
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(_tid, _pid, Permissions.READ_ONLY);
                _it = page.iterator(); // 获取当前页面的迭代器
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                // 用while的原因是要跳过所有的空页，直到找到一个有内容的页
                while (true) {
                    if (_it == null)
                        return false; // 迭代器为空，说明没有打开
                    if (_it.hasNext())
                        return true; // 如果当前页还有元素，返回true
                    // 当前页没有元素了，需要判断是否还有下一页
                    if (_pid.getPageNumber() == numPages() - 1)
                        return false; // 如果当前页是最后一页，返回false
                    // 如果不是最后一页，需要获取下一页
                    _pid = new HeapPageId(getId(), _pid.getPageNumber() + 1); // 获取下一页的PageId
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(_tid, _pid, Permissions.READ_ONLY);
                    _it = page.iterator(); // 获取下一页的迭代器
                }
            }
            
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext())
                    throw new NoSuchElementException(); // 如果没有下一个元素，抛出异常
                return _it.next(); // 返回页面内部迭代器的下一个元素
            }

            @Override // 将迭代器设置到开头，本例中重新打开即可
            public void rewind() throws DbException, TransactionAbortedException {
                // 重新打开迭代器
                open();
            }

            @Override
            public void close() {
                // code done
                _it = null; // 将迭代器设置为null
                _pid = null; // 将PageId设置为null
                _tid = null; // 将事务ID设置为null
            }
        };
        return it;
    }

}

