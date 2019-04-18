package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.nio.file.Path;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.utils.Utils;

public class DictionaryManagerConstants {

    public static final String TABLE_NAME = "dictionary";

    public static final Path INDEX_DIR = Utils.getDefaultIndexDirectory().resolve("dictionaries");
    
    public static final Path DICTIONARY_DIR_PATH = Utils.getTexeraHomePath().resolve("user-resources").resolve("dictionaries");
    public static final String DICTIONARY_DIR = DICTIONARY_DIR_PATH.toString();

    public static final String NAME = "name";
    public static final Attribute NAME_ATTR = new Attribute(NAME, AttributeType.STRING);
    
    public static final Schema SCHEMA = new Schema(NAME_ATTR);
    
}
