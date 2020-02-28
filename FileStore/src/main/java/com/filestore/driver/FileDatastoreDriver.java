package com.filestore.driver;

import org.codehaus.jettison.json.JSONObject;

import com.filestore.bean.DataStoreVO;
import com.filestore.monitor.DataStoreMonitoringProcessDriver;

public class FileDatastoreDriver
{

	public static void main(String[] args) {
		try {
			DataStoreVO dataStore = DataStoreVO.getInstance("");
			JSONObject jsonObject = null;
			for (int i = 0; i < 20; i++) {
				String key = "key_" + i;
				int expiryTime = i + 60;
				jsonObject = new JSONObject();
				jsonObject.put("name", "processOne");
				jsonObject.put("id", i);
				jsonObject.put("size", i * i);
				dataStore.create(jsonObject, key, expiryTime);
			}

			for (int i = 0; i < 5; i++) {
				String key = "key_" + i;
				jsonObject = new JSONObject();
				jsonObject.put("name", "processOne");
				jsonObject.put("id", i);
				jsonObject.put("size", i * i);
				dataStore.delete(key,dataStore.getDataStoreLocation());
			}

			for (int i = 0; i < 20; i++) {
				String key = "key_" + i;
				jsonObject = new JSONObject();
				jsonObject.put("name", "processOne");
				jsonObject.put("id", i);
				jsonObject.put("size", i * i);
				dataStore.readFromStore(key);
			}

			dataStore.checkTaskCompletion();
			
			DataStoreMonitoringProcessDriver.init(dataStore.getDataStoreLocation());
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			System.out.println("Process done..");
		}
	}

}
