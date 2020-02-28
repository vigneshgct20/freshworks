package com.filestore.bean;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONObject;

import com.filestore.callable.CreateCallable;
import com.filestore.callable.DeleteCallable;
import com.filestore.callable.ReadCallable;
import com.filestore.common.DatastoreConstants;

public class DataStoreVO {
	private static DataStoreVO dataStoreVO = null;

	private String dataStoreLocation = "";

	private static File dataStoreDir = null;

	private static File dataStore = null;

	private static final Object OBJECTLOCK = new Object();
	
	private static final int NO_OF_THREADS_EXECUTOR_ONE = Integer.parseInt(System.getProperty("EXECUTOR_POOL_ONE", "10"));

	private static final int NO_OF_THREADS_EXECUTOR_TWO = Integer.parseInt(System.getProperty("EXECUTOR_POOL_TWO", "10"));

	private static final int NO_OF_THREADS_EXECUTOR_THREE = Integer.parseInt(System.getProperty("EXECUTOR_POOL_THREE", "10"));

	private static final ExecutorService EXECUTOR_POOL_ONE = Executors.newFixedThreadPool(NO_OF_THREADS_EXECUTOR_ONE);

	private static final ExecutorService EXECUTOR_POOL_TWO = Executors.newFixedThreadPool(NO_OF_THREADS_EXECUTOR_TWO);

	private static final ExecutorService EXECUTOR_POOL_THREE = Executors.newFixedThreadPool(NO_OF_THREADS_EXECUTOR_THREE);
	
	private static List<Future<Boolean>> executorPoolOneList = new ArrayList<Future<Boolean>>();
	private static List<Future<Boolean>> executorPoolTwoList = new ArrayList<Future<Boolean>>();
	private static List<Future<JSONObject>> executorPoolThreeList = new ArrayList<Future<JSONObject>>();

	public String getDataStoreLocation() {
		return dataStoreLocation;
	}

	public void setDataStoreLocation(String dataStoreLocation) {
		this.dataStoreLocation = dataStoreLocation;
	}

	private DataStoreVO() {

	}

	private static void init(String storeLocation) {
		try {
			dataStoreVO = new DataStoreVO();
			if (StringUtils.isBlank(storeLocation)) {
				dataStoreDir = new File(DatastoreConstants.TEMP_RELATIVE_FILE_DIRECTORY);
				if (!dataStoreDir.exists()) {
					dataStoreDir.mkdirs();
				}
				storeLocation = DatastoreConstants.TEMP_RELATIVE_FILE_DIRECTORY.concat(File.separator)
						.concat(DatastoreConstants.TEMP_RELATIVE_FILE_NAME);
			}
			dataStore = new File(storeLocation);
			dataStoreVO.setDataStoreLocation(storeLocation);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static synchronized DataStoreVO getInstance(String storeLocation) {
		synchronized (OBJECTLOCK) {
			if (dataStoreVO == null) {
				init(storeLocation);
			}
		}
		return dataStoreVO;
	}

	public void create(JSONObject jsonObject, String key, int expiryTime) {
		try {
			CreateCallable createCallable = new CreateCallable(jsonObject, key, expiryTime,dataStoreVO.getDataStoreLocation());
			executorPoolOneList.add(EXECUTOR_POOL_ONE.submit(createCallable));
		}catch(Exception e) {
			e.printStackTrace();
		}
	}


	public void delete(String key, String string) {
		try {
			DeleteCallable deleteCallable = new DeleteCallable(key,dataStoreVO.getDataStoreLocation());
			executorPoolTwoList.add(EXECUTOR_POOL_TWO.submit(deleteCallable));
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void readFromStore(String key) {
		try {
			ReadCallable readCallable = new ReadCallable(key,dataStoreVO.getDataStoreLocation());
			executorPoolThreeList.add(EXECUTOR_POOL_THREE.submit(readCallable));
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void checkTaskCompletion() {
		try {
			for(Future<Boolean> f:executorPoolOneList) {
				if (f.get() != null) {
					System.out.println("Done..");
				}
			}
			
			for(Future<Boolean> f:executorPoolTwoList) {
				if (f.get() != null) {
					System.out.println("Done..");
				}
			}
			
			for(Future<JSONObject> f:executorPoolThreeList) {
				if (f.get() != null) {
					System.out.println("Done..");
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
}
