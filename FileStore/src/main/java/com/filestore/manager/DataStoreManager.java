package com.filestore.manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONObject;

import com.filestore.bean.DataStoreVO;
import com.filestore.common.DatastoreConstants;
import com.filestore.common.StoreException;

public class DataStoreManager
{

	private DataStoreManager() {
		
	}
	
	private synchronized boolean isKeyExist(String key, String storeLocation) throws Exception {
		boolean isKeyExist = false;
		try (InputStream inpustStream = new FileInputStream(new File(storeLocation)); ObjectInputStream objectInputStream = new ObjectInputStream(inpustStream)) {
			Map<String, JSONObject> map = (Map<String, JSONObject>) objectInputStream.readObject();
			if (!map.isEmpty() && map.containsKey(key)) {
				isKeyExist = true;
			}
		} catch (Exception e) {
			throw e;
		}
		return isKeyExist;
	}

	private String create(String key, JSONObject jsonToPut, String storeLocation) throws Exception {
		return create(key, jsonToPut, storeLocation, DatastoreConstants.DEFAULT_KEY_TIME_TO_EXIST);
	}

	private synchronized String create(String key, JSONObject jsonToPut, String storeLocation, int expiryTime) throws Exception {
		String message = "";
		File dataStoreFile = new File(storeLocation);
		try (FileInputStream inpustStream = new FileInputStream(dataStoreFile); ObjectInputStream objectInputStream = new ObjectInputStream(inpustStream);) {
			boolean isKeyAlreadyPresent = isKeyExist(key, storeLocation);
			if (isKeyAlreadyPresent) {
				throw new StoreException(DatastoreConstants.KEY_EXISTS);
			}
			if (isKeyValid(key)) {
				Map<String, JSONObject> map = (Map<String, JSONObject>) objectInputStream.readObject();
				if (map == null) {
					map = new HashMap<>();
				}
				map.put(key, jsonToPut);
				int count = 0;
				while (count < DatastoreConstants.WRITE_RETRY_COUNT) {
					try (FileOutputStream outputStream = new FileOutputStream(dataStoreFile); FileLock fLock = outputStream.getChannel().tryLock(); ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
						if (null == fLock) {
							throw new StoreException("FILE_LOCKED_ALREADY");
						}
						objectOutputStream.writeObject(map);
					} catch (Exception e) {
						count++;
						Thread.sleep(1500L);
					}
				}
				writeExpiryFile(key, storeLocation, expiryTime);
			} else {
				throw new StoreException(DatastoreConstants.KEY_NOT_VALID);
			}
		} catch (Exception e) {
			throw e;
		}
		return message;
	}

	private synchronized void writeExpiryFile(String key, String storeLocation, int expiryTime) throws Exception {
		try {
			String storeDirs = storeLocation.substring(0, storeLocation.lastIndexOf(File.separatorChar));
			File dir = new File(storeDirs);
			if (dir.exists()) {
				dir.mkdirs();
			}
			String expiryFile = storeDirs.concat(File.separator).concat(DatastoreConstants.EXPIRY_FILE_NAME);
			try (FileInputStream inpustStream = new FileInputStream(new File(expiryFile)); ObjectInputStream objectInputStream = new ObjectInputStream(inpustStream);) {
				Map<String, String> map = (Map<String, String>) objectInputStream.readObject();
				if (map == null) {
					map = new HashMap<>();
				}
				map.put(key, String.valueOf(System.currentTimeMillis()).concat("_").concat(String.valueOf(expiryTime)));
				int count = 0;
				while (count < DatastoreConstants.WRITE_RETRY_COUNT) {
					try (FileOutputStream outputStream = new FileOutputStream(new File(expiryFile)); FileLock fLock = outputStream.getChannel().tryLock(); ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);) {
						if (null == fLock) {
							throw new StoreException("FILE_LOCKED_ALREADY");
						}
						objectOutputStream.writeObject(map);
					} catch (Exception e) {
						count++;
						Thread.sleep(1500L);
					}
				}

			} catch (Exception e) {
				throw e;
			}
		} catch (Exception e) {
			throw e;
		}
	}

	private synchronized boolean isKeyValid(String key) throws StoreException {
		boolean isKeyValid = false;
		try {
			if (StringUtils.isNotBlank(key) && key.length() <= DatastoreConstants.KEY_MAX_LENGTH) {
				isKeyValid = true;
			}
		} catch (Exception e) {
			throw new StoreException(e);
		}
		return isKeyValid;
	}

	private synchronized boolean delete(String key, String storeLocation) throws Exception {
		boolean isDeleted = false;
		File dataStoreFile = new File(storeLocation);
		try (FileInputStream inpustStream = new FileInputStream(dataStoreFile); ObjectInputStream objectInputStream = new ObjectInputStream(inpustStream);) {
			boolean isKeyPresent = isKeyExist(key, storeLocation);
			if (!isKeyPresent) {
				throw new StoreException(DatastoreConstants.KEY_NOT_EXISTS);
			}
			Map<String, JSONObject> map = (Map<String, JSONObject>) objectInputStream.readObject();
			if (map != null && !map.isEmpty()) {
				map.remove(key);
			}
			int count = 0;
			while (count < DatastoreConstants.WRITE_RETRY_COUNT) {
				try (FileOutputStream outputStream = new FileOutputStream(dataStoreFile); FileLock fLock = outputStream.getChannel().tryLock(); ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
					if (null == fLock) {
						throw new StoreException("FILE_LOCKED_ALREADY");
					}
					objectOutputStream.writeObject(map);
				} catch (Exception e) {
					count++;
					Thread.sleep(1500L);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return isDeleted;
	}

	private synchronized JSONObject readFromStore(String key, String storeLocation) throws Exception {
		JSONObject response = new JSONObject();
		try (FileInputStream inpustStream = new FileInputStream(new File(storeLocation)); ObjectInputStream objectInputStream = new ObjectInputStream(inpustStream);) {
			Map<String, JSONObject> map = (Map<String, JSONObject>) objectInputStream.readObject();
			for (Entry<String, JSONObject> entry : map.entrySet()) {
				if (entry.getKey().equals(key)) {
					response = entry.getValue();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return response;
	}

	private void checkAndRemoveExpiredKeys(String storeLocation) {
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

		}
		return isExpired;
	}

}
