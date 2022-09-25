package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
    // singleton pattern: only one catalog is allowed
    private ConcurrentHashMap<Integer, DbFile> _dbFiles;
    private ConcurrentHashMap<String, Integer> _tableIds; // maps table name to table id
    private ConcurrentHashMap<Integer, String> _tableNames; // maps table id to table name
    private ConcurrentHashMap<Integer, String> _primaryKeys; // maps table id to primary key

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        _dbFiles = new ConcurrentHashMap<Integer, DbFile>();
        _tableIds = new ConcurrentHashMap<String, Integer>();
        _tableNames = new ConcurrentHashMap<Integer, String>();
        _primaryKeys = new ConcurrentHashMap<Integer, String>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * 
     * @param file      the contents of the table to add; file.getId() is the
     *                  identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and
     *                  getFile
     * @param name      the name of the table -- may be an empty string. May not be
     *                  null. If a name
     *                  conflict exists, use the last table to be added as the table
     *                  for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // code done
        if (name == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        if (pkeyField == null) {
            throw new IllegalArgumentException("Primary key field cannot be null");
        }
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }
        int tableId = file.getId(); // 新的id
        // 如果这个文件的id已经有了，那么把原来的文件名替换成新的
        if (_tableIds.containsValue(tableId)) {
            String oldName = _tableNames.get(tableId);
            _tableNames.remove(tableId);
            _tableIds.remove(oldName);
            _tableNames.put(tableId, name);
            _tableIds.put(name, tableId);
        } else {
            _tableIds.put(name, tableId);
            _tableNames.put(tableId, name);
        }
        _primaryKeys.put(tableId, pkeyField);
        _dbFiles.put(tableId, file);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * 
     * @param file the contents of the table to add; file.getId() is the identfier
     *             of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * 
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // code done
        if (name == null || !_tableIds.containsKey(name)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return _tableIds.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // code done
        // iterate the _tableIds
        // System.out.println("Get tuple Desc tableid: " + tableid);
        // for (Map.Entry<String, Integer> entry : _tableIds.entrySet()) {
        // System.out.println(entry.getKey() + " " + entry.getValue());
        // }
        if (!_tableIds.containsValue(tableid)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return _dbFiles.get(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // code done
        if (!_tableIds.containsValue(tableid)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return _dbFiles.get(tableid);
    }

    public String getPrimaryKey(int tableid) {
        // code done
        if (!_primaryKeys.containsKey(tableid)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return _primaryKeys.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        // code done
        return _tableIds.values().iterator();
    }

    public String getTableName(int id) {
        // code done
        if (!_tableNames.containsKey(id)) {
            throw new NoSuchElementException("Table does not exist");
        }
        return _tableNames.get(id);
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        _dbFiles.clear();
        _tableIds.clear();
        _tableNames.clear();
        _primaryKeys.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the
     * database.
     * 
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            while ((line = br.readLine()) != null) {
                // assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                // System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
