package edu.uci.ics.textdb.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.field.StringField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.storage.utils.StorageUtils;

public class RelationManager {

    private static volatile RelationManager singletonRelationManager = null;

    private RelationManager() throws StorageException {
        if (!checkCatalogExistence()) {
            initializeCatalog();
        }
    }

    public static RelationManager getRelationManager() throws StorageException {
        if (singletonRelationManager == null) {
            synchronized (RelationManager.class) {
                if (singletonRelationManager == null) {
                    singletonRelationManager = new RelationManager();
                }
            }
        }
        return singletonRelationManager;
    }

    /**
     * Checks if a table exists by looking it up in the catalog.
     *
     * @param tableName
     * @return
     */
    public boolean checkTableExistence(String tableName) {
        try {
            tableName = tableName.toLowerCase();
            return getTableCatalogTuple(tableName) != null;
        } catch (StorageException e) {
            // TODO: change it to textdb runtime exception
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new table.
     * Table name must be unique (case insensitive).
     * LuceneAnalyzer must be a valid analyzer string.
     * <p>
     * The "_id" attribute will be added to the table schema.
     * System automatically generates a unique ID for each tuple inserted to a table,
     * the generated ID will be in "_id" field.
     *
     * @param tableName,            the name of the table, must be unique, case is not sensitive
     * @param indexDirectory,       the directory to store the index and data, must not duplicate with other tables' directories
     * @param schema,               the schema of the table
     * @param luceneAnalyzerString, the string representing the lucene analyzer used
     * @throws StorageException
     */
    public void createTable(String tableName, String indexDirectory, Schema schema, String luceneAnalyzerString)
            throws StorageException {
        // convert the table name to lower case
        tableName = tableName.toLowerCase();
        // table should not exist
        if (checkTableExistence(tableName)) {
            throw new StorageException(String.format("Table %s already exists.", tableName));
        }

        // create folder if it's not there
        // and convert the index directory to its absolute path
        try {
            Path indexPath = Paths.get(indexDirectory);
            if (Files.notExists(indexPath)) {
                Files.createDirectories(indexPath);
            }
            indexDirectory = indexPath.toRealPath().toString();
        } catch (IOException e) {
            throw new StorageException(e);
        }

        // check if the indexDirectory overlaps with another table's index directory
        Query indexDirectoryQuery = new TermQuery(new Term(CatalogConstants.TABLE_DIRECTORY, indexDirectory));
        DataReader tableCatalogDataReader = new DataReader(CatalogConstants.TABLE_CATALOG_DATASTORE, indexDirectoryQuery);
        tableCatalogDataReader.setPayloadAdded(false);

        tableCatalogDataReader.open();
        Tuple nextTuple = tableCatalogDataReader.getNextTuple();
        tableCatalogDataReader.close();

        // if the index directory is already taken by another table, throws an exception
        if (nextTuple != null) {
            String overlapTableName = nextTuple.getField(CatalogConstants.TABLE_NAME).getValue().toString();
            throw new StorageException(String.format(
                    "Table %s already takes the index directory %s. Please choose another directory.",
                    overlapTableName, indexDirectory));
        }

        // check if the lucene analyzer string is valid
        Analyzer luceneAnalyzer = null;
        try {
            luceneAnalyzer = LuceneAnalyzerConstants.getLuceneAnalyzer(luceneAnalyzerString);
        } catch (DataFlowException e) {
            throw new StorageException("Lucene Analyzer String is not valid.");
        }

        // create the directory and clear all data in the index directory
        Schema tableSchema = Utils.getSchemaWithID(schema);
        DataStore tableDataStore = new DataStore(indexDirectory, tableSchema);
        DataWriter dataWriter = new DataWriter(tableDataStore, luceneAnalyzer);
        dataWriter.open();
        dataWriter.clearData();
        dataWriter.close();

        // write table info to catalog
        writeTableInfoToCatalog(tableName, indexDirectory, schema, luceneAnalyzerString);

    }

    /**
     * Deletes a table by its name.
     * If the table doesn't exist, it won't do anything.
     * Deleting system catalog tables is prohibited.
     *
     * @param tableName, the name of a table, case insensitive
     * @throws StorageException
     */
    public void deleteTable(String tableName) throws StorageException {
        tableName = tableName.toLowerCase();
        // User can't delete catalog table
        if (isSystemCatalog(tableName)) {
            throw new StorageException("Deleting a system catalog table is prohibited.");
        }
        // if table doesn't exist, then do nothing
        if (!checkTableExistence(tableName)) {
            return;
        }

        // try to clear all data in the table
        DataWriter dataWriter = getTableDataWriter(tableName);
        dataWriter.open();
        dataWriter.clearData();
        dataWriter.close();
        StorageUtils.deleteDirectory(getTableDirectory(tableName));

        // generate a query for the table name
        Query catalogTableNameQuery = new TermQuery(new Term(CatalogConstants.TABLE_NAME, tableName));

        // delete the table from table catalog
        DataWriter tableCatalogWriter = new DataWriter(CatalogConstants.TABLE_CATALOG_DATASTORE,
                LuceneAnalyzerConstants.getStandardAnalyzer());
        tableCatalogWriter.open();
        tableCatalogWriter.deleteTuple(catalogTableNameQuery);
        tableCatalogWriter.close();

        // delete the table from schema catalog
        DataWriter schemaCatalogWriter = new DataWriter(CatalogConstants.SCHEMA_CATALOG_DATASTORE,
                LuceneAnalyzerConstants.getStandardAnalyzer());
        schemaCatalogWriter.open();
        schemaCatalogWriter.deleteTuple(catalogTableNameQuery);
        schemaCatalogWriter.close();

    }

    /**
     * Gets a tuple in a table by its _id field.
     * Returns null if the tuple doesn't exist.
     *
     * @param tableName, the name of the table, case insensitive
     * @param idValue,   the IDField to lookup
     * @return
     * @throws StorageException
     */
    public Tuple getTupleByID(String tableName, IDField idField) throws StorageException {
        // construct the ID query
        Query tupleIDQuery = new TermQuery(new Term(SchemaConstants._ID, idField.getValue().toString()));

        // find the tuple using DataReader
        DataReader dataReader = getTableDataReader(tableName, tupleIDQuery);
        dataReader.setPayloadAdded(false);

        dataReader.open();
        Tuple tuple = dataReader.getNextTuple();
        dataReader.close();

        return tuple;
    }

    /**
     * Gets the DataWriter of a table.
     * The DataWriter can be used to insert/delete/update tuples in a table.
     *
     * @param tableName, the name of the table, case insensitive
     * @return
     * @throws StorageException
     */
    public DataWriter getTableDataWriter(String tableName) throws StorageException {
        if (isSystemCatalog(tableName)) {
            throw new StorageException("modify system catalog is not allowed");
        }
        return new DataWriter(getTableDataStore(tableName), getTableAnalyzer(tableName));
    }

    /**
     * Gets a DataReader for a table based on a query.
     * DataReader can return tuples that match the query.
     *
     * @param tableName,  the name of a table, case insensitive
     * @param tupleQuery, the query to run on the table
     * @return
     * @throws StorageException
     */
    public DataReader getTableDataReader(String tableName, Query tupleQuery) throws StorageException {
        DataStore tableDataStore = getTableDataStore(tableName);
        return new DataReader(tableDataStore, tupleQuery);
    }

    /**
     * Gets the DataStore(directory and schema) of a table.
     *
     * @param tableName, the name of the table, case insensitive
     * @return
     * @throws StorageException
     */
    public DataStore getTableDataStore(String tableName) throws StorageException {
        String tableDirectory = getTableDirectory(tableName);
        Schema tableSchema = getTableSchema(tableName);
        return new DataStore(tableDirectory, tableSchema);
    }

    /**
     * Gets the directory of a table.
     *
     * @param tableName, the name of the table, case insensitive
     * @return
     * @throws StorageException
     */
    public String getTableDirectory(String tableName) throws StorageException {
        // get the tuples with tableName from the table catalog
        Tuple tableCatalogTuple = getTableCatalogTuple(tableName);

        // if the tuple is not found, then the table name is not found
        if (tableCatalogTuple == null) {
            throw new StorageException(String.format("The directory for table %s is not found.", tableName));
        }

        // get the directory field
        IField directoryField = tableCatalogTuple.getField(CatalogConstants.TABLE_DIRECTORY);
        return directoryField.getValue().toString();
    }

    /**
     * Gets the schema of a table.
     *
     * @param tableName, the name of the table, case insensitive
     * @return
     * @throws StorageException
     */
    public Schema getTableSchema(String tableName) throws StorageException {
        // get the tuples with tableName from the schema catalog
        List<Tuple> tableAttributeTuples = getSchemaCatalogTuples(tableName);

        // if the list is empty, then the schema is not found
        if (tableAttributeTuples.isEmpty()) {
            throw new StorageException(String.format("The schema of table %s is not found.", tableName));
        }

        // convert the unordered list of tuples to an order list of attributes
        List<Attribute> tableSchemaData = tableAttributeTuples.stream()
                // sort the tuples based on the attributePosition field.
                .sorted((tuple1, tuple2) -> Integer.compare((int) tuple1.getField(CatalogConstants.ATTR_POSITION).getValue(),
                        (int) tuple2.getField(CatalogConstants.ATTR_POSITION).getValue()))
                // map one tuple to one attribute
                .map(tuple -> new Attribute(tuple.getField(CatalogConstants.ATTR_NAME).getValue().toString(),
                        convertAttributeType(tuple.getField(CatalogConstants.ATTR_TYPE).getValue().toString())))
                .collect(Collectors.toList());

        return new Schema(tableSchemaData.stream().toArray(Attribute[]::new));
    }

    /**
     * Gets the Lucene analyzer string of a table.
     *
     * @param tableName, the name of the table, case insensitive
     * @return
     * @throws StorageException
     */
    public String getTableAnalyzerString(String tableName) throws StorageException {
        // get the tuples with tableName from the table catalog
        Tuple tableCatalogTuple = getTableCatalogTuple(tableName);

        // if the tuple is not found, then the table name is not found
        if (tableCatalogTuple == null) {
            throw new StorageException(String.format("The analyzer for table %s is not found.", tableName));
        }

        // get the lucene analyzer string
        IField analyzerField = tableCatalogTuple.getField(CatalogConstants.TABLE_LUCENE_ANALYZER);
        String analyzerString = analyzerField.getValue().toString();

        return analyzerString;
    }

    /**
     * Gets the Lucene analyzer of a table.
     *
     * @param tableName, the name of the table, case insensitive
     * @return
     * @throws StorageException
     */
    public Analyzer getTableAnalyzer(String tableName) throws StorageException {
        String analyzerString = getTableAnalyzerString(tableName);

        // convert a lucene analyzer string to an analyzer object
        Analyzer luceneAnalyzer = null;
        try {
            luceneAnalyzer = LuceneAnalyzerConstants.getLuceneAnalyzer(analyzerString);
        } catch (DataFlowException e) {
            throw new StorageException(e);
        }

        return luceneAnalyzer;
    }

    /*
     * This is a helper function that writes the table information to 
     *   the table catalog and the schema catalog.
     */
    private void writeTableInfoToCatalog(String tableName, String indexDirectory, Schema schema, String luceneAnalyzerString)
            throws StorageException {
        // write table catalog
        DataStore tableCatalogStore = new DataStore(CatalogConstants.TABLE_CATALOG_DIRECTORY,
                CatalogConstants.TABLE_CATALOG_SCHEMA);
        DataWriter dataWriter = new DataWriter(tableCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer());
        dataWriter.open();
        dataWriter.insertTuple(CatalogConstants.getTableCatalogTuple(tableName, indexDirectory, luceneAnalyzerString));
        dataWriter.close();

        // write schema catalog
        Schema tableSchema = Utils.getSchemaWithID(schema);
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA);
        dataWriter = new DataWriter(schemaCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer());
        // each attribute in the table schema will be one row in schema catalog
        dataWriter.open();
        for (Tuple tuple : CatalogConstants.getSchemaCatalogTuples(tableName, tableSchema)) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
    }

    /*
     * Gets the a tuple of a table from table catalog.
     */
    private static Tuple getTableCatalogTuple(String tableName) throws StorageException {
        tableName = tableName.toLowerCase();

        Query tableNameQuery = new TermQuery(new Term(CatalogConstants.TABLE_NAME, tableName));
        DataReader tableCatalogDataReader = new DataReader(CatalogConstants.TABLE_CATALOG_DATASTORE, tableNameQuery);
        tableCatalogDataReader.setPayloadAdded(false);

        tableCatalogDataReader.open();
        List<Tuple> tupleList = new ArrayList<>();
        Tuple nextTuple;
        while ((nextTuple = tableCatalogDataReader.getNextTuple()) != null) {
            tupleList.add(nextTuple);
        }
        tableCatalogDataReader.close();

        if (tupleList.size() == 0) {
            return null;
        } else if (tupleList.size() == 1) {
            return tupleList.get(0);
        } else {
            throw new StorageException("Catalog corrupted: duplicate table name found in catalog.");
        }
    }

    /*
     * Gets the tuples of a table from schema catalog.
     */
    private static List<Tuple> getSchemaCatalogTuples(String tableName) throws StorageException {
        tableName = tableName.toLowerCase();

        Query tableNameQuery = new TermQuery(new Term(CatalogConstants.TABLE_NAME, tableName));
        DataReader schemaCatalogDataReader = new DataReader(CatalogConstants.SCHEMA_CATALOG_DATASTORE, tableNameQuery);

        // read the tuples into a list
        schemaCatalogDataReader.open();
        List<Tuple> tupleList = new ArrayList<>();
        Tuple nextTuple;
        while ((nextTuple = schemaCatalogDataReader.getNextTuple()) != null) {
            tupleList.add(nextTuple);
        }
        schemaCatalogDataReader.close();

        return tupleList;
    }

    /*
     * This is a helper function to check if the system catalog tables exist physically on the disk.
     */
    private static boolean checkCatalogExistence() {
        return DataReader.checkIndexExistence(CatalogConstants.TABLE_CATALOG_DIRECTORY)
                && DataReader.checkIndexExistence(CatalogConstants.SCHEMA_CATALOG_DIRECTORY);
    }

    /*
     * This is a helper function to check if the table is a system catalog table.
     */
    private static boolean isSystemCatalog(String tableName) {
        return tableName.equalsIgnoreCase(CatalogConstants.TABLE_CATALOG)
                || tableName.equalsIgnoreCase(CatalogConstants.SCHEMA_CATALOG);
    }

    /*
     * Initializes the system catalog tables.
     */
    private void initializeCatalog() throws StorageException {
        try {
            // create table catalog
            writeTableInfoToCatalog(CatalogConstants.TABLE_CATALOG.toLowerCase(),
                    new File(CatalogConstants.TABLE_CATALOG_DIRECTORY).getCanonicalPath(),
                    CatalogConstants.TABLE_CATALOG_SCHEMA,
                    LuceneAnalyzerConstants.standardAnalyzerString());
            // create schema catalog
            writeTableInfoToCatalog(CatalogConstants.SCHEMA_CATALOG.toLowerCase(),
                    new File(CatalogConstants.SCHEMA_CATALOG_DIRECTORY).getCanonicalPath(),
                    CatalogConstants.SCHEMA_CATALOG_SCHEMA,
                    LuceneAnalyzerConstants.standardAnalyzerString());
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }


    /*
     * Converts a attributeTypeString to AttributeType (case insensitive).
     * It returns null if string is not a valid type.
     * 
     */
    private static AttributeType convertAttributeType(String attributeTypeStr) {
        return Stream.of(AttributeType.values())
                .filter(typeStr -> typeStr.toString().equalsIgnoreCase(attributeTypeStr))
                .findAny().orElse(null);
    }

    public List<TableMetadata> getMetaData() throws Exception {
        DataReader dataReader = RelationManager.getRelationManager().getTableDataReader(CatalogConstants.TABLE_CATALOG, new MatchAllDocsQuery());

        List<TableMetadata> result = new ArrayList<>();
        Tuple t = null;
        dataReader.open();
        while ((t = dataReader.getNextTuple()) != null) {
            String tableName = (String) t.getField(CatalogConstants.TABLE_NAME).getValue();

            if (!tableName.equals(CatalogConstants.SCHEMA_CATALOG.toLowerCase())
                    && !tableName.equals(CatalogConstants.TABLE_CATALOG.toLowerCase())
                    && !tableName.equals("dictionary")) {
                result.add(new TableMetadata(tableName, getTableSchema(tableName)));
            }
        }
        dataReader.close();

        return result;
    }

    /**
     * Add a new dictionary metadata into dictionary table
     * If it exists, remove it and add a new tuple.
     *
     * @param fileUploadDirectory
     * @param fileName
     * @throws Exception
     */
    public void addDictionaryTable(String fileUploadDirectory, String fileName) throws Exception {
        RelationManager relationManager = getRelationManager();
        String tableName = "dictionary";
        String indexDirectory = "dictionary";
        Schema dictionarySchema = new Schema(new Attribute("name", AttributeType.STRING)
                , new Attribute("path", AttributeType.STRING));

        // check if there is a dictionary table. If not, create a new one.
        if (!relationManager.checkTableExistence(tableName)) {
            relationManager.createTable(tableName, indexDirectory,
                    dictionarySchema, LuceneAnalyzerConstants.standardAnalyzerString());
        }

        // open both reader and writer to remove existing dictionary metadata (if so) and add a new one.
        DataReader dataReader = relationManager.getTableDataReader(tableName, new MatchAllDocsQuery());
        dataReader.open();

        DataWriter dataWriter = relationManager.getTableDataWriter(tableName);
        dataWriter.open();

        // get all existing dictionary metadata
        HashMap<IDField, String> tuples = new HashMap<>();
        Tuple t;
        while ((t = dataReader.getNextTuple()) != null) {
            tuples.put((IDField)t.getField("_id"),
                    ((String)t.getField("path").getValue()).concat((String)t.getField("name").getValue()));
        }
        dataReader.close();

        // clean up if the same dictionary metadata already exists in dictionary table
        if (tuples.containsValue(fileUploadDirectory.concat(fileName))) {
            tuples.entrySet().forEach(tuple -> {
                if (tuple.getValue().equals(fileUploadDirectory.concat(fileName))) {
                    dataWriter.deleteTupleByID(tuple.getKey());
                }
            });
        }

        // add a new dictionary metadata
        Tuple tuple = new Tuple(dictionarySchema, new StringField(fileName)
                , new StringField(fileUploadDirectory));
        dataWriter.insertTuple(tuple);
        dataWriter.close();
    }

	public HashMap<String, String> getDictionaries() {
      String tableName = "dictionary";

      DataReader dataReader = RelationManager.getRelationManager().getTableDataReader(tableName, new MatchAllDocsQuery());
      dataReader.open();

      HashMap<String, String> tuples = new HashMap<>();
      Tuple t;
      while ((t = dataReader.getNextTuple()) != null) {
          tuples.put(((String)t.getField("name").getValue()), (String)t.getField("_id").getValue());
      }
      dataReader.close();

      return tuples;
	}

    public String getDictionaryPath(String id) {
        String tableName = "dictionary";

        IDField idField = new IDField(id);
        Tuple tuple = RelationManager.getRelationManager().getTupleByID(tableName, idField);

        if (tuple == null) return null;

        String fullPath = ((String)tuple.getField("path").getValue()).concat((String)tuple.getField("name").getValue());

        return fullPath;
    }
}