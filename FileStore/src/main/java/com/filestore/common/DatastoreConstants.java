package com.filestore.common;

import java.io.File;

public class DatastoreConstants
{
	public static final String KEY_EXISTS = "Key Already Exists.";
	
	public static final String KEY_NOT_EXISTS = "Key doesn't Exists.";
	
	public static final String KEY_NOT_VALID = "Key size exceeded Limit / Key may be Empty.";

	public static final String TEMP_RELATIVE_FILE_DIRECTORY = "tmp".concat(File.separator).concat("store");

	public static final String TEMP_RELATIVE_FILE_NAME = "Datastore.ser";

	public static final int KEY_MAX_LENGTH = 32;
	
	public static final int VALUE_MAX_SIZE_IN_KB = 16;
	
	public static final int DEFAULT_KEY_TIME_TO_EXIST = 5;
	
	public static final String EXPIRY_FILE_NAME = "KeyExpiry.ser";

	public static final int WRITE_RETRY_COUNT = 5;
	
	public static final double SIZE_1GB = 1073741824D;
	
}
