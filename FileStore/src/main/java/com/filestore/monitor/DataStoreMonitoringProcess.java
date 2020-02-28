package com.filestore.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONObject;

import com.filestore.common.DatastoreConstants;
import com.filestore.common.StoreException;
import com.filestore.manager.DataStoreManager;

public class DataStoreMonitoringProcess implements Runnable
{

	private static final String PROCESS_NAME = "DataStoreMonitoringProcess";

	private String storeLocation = null;

	public DataStoreMonitoringProcess(String storeLocation) {
		this.storeLocation = storeLocation;
	}

	@Override
	public void run() {
		checkAndRemoveExpiredKeys();
	}

	private void checkAndRemoveExpiredKeys() {
		try {
			String storeDirs = storeLocation.substring(0, storeLocation.lastIndexOf(File.separatorChar));
			String expiryFile = storeDirs.concat(File.separator).concat(DatastoreConstants.EXPIRY_FILE_NAME);
			File expiry = new File(expiryFile);
			File storeFile = new File(storeLocation);
			Map<String, JSONObject> map = null;
			Map<String, String> expiryMap = null;
			try (FileInputStream inpustStream = new FileInputStream(storeFile); ObjectInputStream objectInputStream = new ObjectInputStream(inpustStream); FileInputStream expiryInpustStream = new FileInputStream(expiry); ObjectInputStream expiryObjectInputStream = new ObjectInputStream(expiryInpustStream);) {
				map = (Map<String, JSONObject>) objectInputStream.readObject();
				expiryMap = (Map<String, String>) expiryObjectInputStream.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}

			int count = 0;
			while (count < DatastoreConstants.WRITE_RETRY_COUNT) {
				try (FileOutputStream outputStream = new FileOutputStream(storeFile); FileLock fLock = outputStream.getChannel().tryLock(); ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream); FileOutputStream expiryOutputStream = new FileOutputStream(expiry); FileLock fLockExpiry = outputStream.getChannel().tryLock(); ObjectOutputStream expiryObjectOutputStream = new ObjectOutputStream(outputStream);) {
					if (fLock == null || fLockExpiry == null) {
						throw new StoreException("FILE_LOCK_NOT_AQUIRED");
					}
					Map<String, String> newExpiryMap = new HashMap<>();
					for (Entry<String, String> entry : expiryMap.entrySet()) {
						if (isKeyExpired(entry.getValue())) {
							map.remove(entry.getKey());
						} else {
							newExpiryMap.put(entry.getKey(), entry.getValue());
						}
					}
					objectOutputStream.writeObject(map);
					expiryObjectOutputStream.writeObject(newExpiryMap);
				} catch (Exception e) {
					count++;
					Thread.sleep(1500L);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private synchronized boolean isKeyExpired(String value) {
		boolean isExpired = false;
		try {
			if (StringUtils.isNotBlank(value)) {
				String[] times = value.split("_");
				int currentTime = Integer.parseInt(String.valueOf(System.currentTimeMillis()));
				int keyCreatedTime = Integer.parseInt(times[0]);
				int keyTimeToExist = Integer.parseInt(times[1]);
				if ((keyCreatedTime + keyTimeToExist) >= currentTime) {
					isExpired = true;
				}
			} else {
				throw new StoreException("NOT_EXPIRY_DETAILS_AVAILABLE");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isExpired;
	}

}
