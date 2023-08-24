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
import java.util.function.Consumer;

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

    /**
     * Table description, includes a name, a primary key field name, a DbFile reference
     */
    private static class TableDesc {
        private DbFile file;
        private String name;
        private String pkeyField;

        public TableDesc() {
        }

        public TableDesc(DbFile file, String name, String pkeyField) {
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }

        public DbFile getFile() {
            return file;
        }

        public String getName() {
            return name;
        }

        public String getPkeyField() {
            return pkeyField;
        }

        public void setFile(DbFile file) {
            this.file = file;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setPkeyField(String pkeyField) {
            this.pkeyField = pkeyField;
        }
    }

    private ArrayList<TableDesc> tableDescs;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // TODO: some code goes here
        tableDescs = new ArrayList<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // TODO: some code goes here
        if (name == null) // 可能为空串,但不能为null; 如果存在名称冲突，使用最后添加的表作为给定名称的表
            throw new IllegalArgumentException("file name cannot be empty");
        int i = 0;
        // 如果存在同样的ID，则替换原来的tableDesc
        for (; i < tableDescs.size(); ++i) {
            int tableId = tableDescs.get(i).getFile().getId();
            if (tableId == file.getId()) {
                tableDescs.set(i, new TableDesc(file, name, pkeyField));
                return;
            }
        }
        // 如果存在同样的name，则替换原来的tableDesc
        i = tableDescs.size() - 1;
        for (; i >= 0; --i) {
            if (name.equals(tableDescs.get(i).getName())) {
                tableDescs.set(i, new TableDesc(file, name, pkeyField));
                return;
            }
        }
        // 否则添加到最后
        tableDescs.add(new TableDesc(file, name, pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * 不会造成重复的表名称，每张表都有一个UUID
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
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
        // TODO: some code goes here
        if (name == null)
            throw new NoSuchElementException();
        for (TableDesc tableDesc : tableDescs)
            if (name.equals(tableDesc.getName()))
                return tableDesc.getFile().getId();
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableId) throws NoSuchElementException {
        // TODO: some code goes here
        for (TableDesc tableDesc : tableDescs)
            if (tableId == tableDesc.getFile().getId())
                return tableDesc.getFile().getTupleDesc();
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableId) throws NoSuchElementException {
        // TODO: some code goes here
        for (TableDesc tableDesc : tableDescs)
            if (tableId == tableDesc.getFile().getId())
                return tableDesc.getFile();
        return null;
    }

    public String getPrimaryKey(int tableId) {
        // TODO: some code goes here
        for (TableDesc tableDesc : tableDescs)
            if (tableId == tableDesc.getFile().getId())
                return tableDesc.getPkeyField();
        return null;
    }

    public String getTableName(int tableId) {
        // TODO: some code goes here
        for (TableDesc tableDesc : tableDescs)
            if (tableId == tableDesc.getFile().getId())
                return tableDesc.getName();
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // TODO: some code goes here
        return new Itr();
    }

    private class Itr implements Iterator<Integer> {
        int cursor;       // index of next element to return
        int lastRet = -1; // index of last element returned; -1 if no such

        // prevent creating a synthetic constructor
        Itr() {
        }

        @Override
        public boolean hasNext() {
            return cursor != tableDescs.size();
        }

        @Override
        public Integer next() {
            int i = cursor;
            if (i >= tableDescs.size())
                throw new NoSuchElementException();
            cursor = i + 1;
            return tableDescs.get(lastRet = i).getFile().getId();
        }

//        @Override
//        public void remove() {
//            if (lastRet < 0)
//                throw new IllegalStateException();
//
//            try {
//                tableDescs.remove(lastRet);
//                cursor = lastRet;
//                lastRet = -1;
//            } catch (IndexOutOfBoundsException ex) {
//                throw new ConcurrentModificationException();
//            }
//        }

        @Override
        public void forEachRemaining(Consumer<? super Integer> action) {
            Objects.requireNonNull(action);
            final int size = tableDescs.size();
            int i = cursor;
            if (i < size) {
                for (; i < size; i++)
                    action.accept(tableDescs.get(i).getFile().getId());
                // update once at end to reduce heap write traffic
                cursor = i;
                lastRet = i - 1;
            }
        }
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // TODO: some code goes here
        tableDescs.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * 从一个文件中读取schema，并在catalog中添加addTable合适的表
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            // 缓冲读
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                List<String> names = new ArrayList<>();
                List<Type> types = new ArrayList<>();
                String primaryKey = ""; // 主键
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string")) // 不同类型
                        types.add(Type.STRING_TYPE);
                    else { // 不知道的类型
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) // 如果该类型是
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

