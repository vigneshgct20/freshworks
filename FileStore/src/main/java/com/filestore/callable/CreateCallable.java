package com.filestore.callable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.codehaus.jettison.json.JSONObject;

import com.filestore.manager.DataStoreManager;

public class CreateCallable implements Callable<Boolean>
{

	private JSONObject jsonObject = null;

	private String key = null;

	private int expiryTime = 0;

	private String storeLoc = null;

	public CreateCallable(JSONObject jsonObject, String key, int expiryTime, String storeLocation) {
		this.jsonObject = jsonObject;
		this.expiryTime = expiryTime;
		this.key = key;
		this.storeLoc = storeLocation;

	}

	public CreateCallable(JSONObject jsonObject, String key, String storeLocation) {
		this.jsonObject = jsonObject;
		this.key = key;
		this.storeLoc = storeLocation;
	}

	@Override
	public Boolean call() throws Exception {
		boolean isDone = true;
		try {
			String className = "com.filestore.manager.DataStoreManager";
			Class<?> classType = Class.forName(className);
			Class<?>[] parameterTypesForConstructor = {};
			Constructor<?> constructorInstance = classType.getConstructor(parameterTypesForConstructor);
			Object[] initArguments = {};
			Object instance = constructorInstance.newInstance(initArguments);

			Class<?>[] parameterTypes = { String.class, JSONObject.class, String.class, int.class };
			Class<?>[] parameterTypesTwo = { String.class, JSONObject.class, String.class };

			Object[] args = { this.key, this.jsonObject, this.storeLoc, this.expiryTime };

			Object[] argsTwo = { this.key, this.jsonObject, this.storeLoc };

			Method methodToInvoke;
			if (expiryTime > 0) {
				methodToInvoke = classType.getMethod("create", parameterTypes);
				methodToInvoke.invoke(instance, args);
			} else {
				methodToInvoke = classType.getMethod("create", parameterTypesTwo);
				methodToInvoke.invoke(instance, argsTwo);
			}

		} catch (Exception e) {
			isDone = false;
			e.printStackTrace();
		}
		return isDone;
	}
}
