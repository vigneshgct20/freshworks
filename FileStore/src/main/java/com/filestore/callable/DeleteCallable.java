package com.filestore.callable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.codehaus.jettison.json.JSONObject;

public class DeleteCallable implements Callable<Boolean>
{

	private String key = null;

	private int expiryTime = 0;

	private String storeLoc = null;

	public DeleteCallable(String key, String dataStoreLocation) {
		this.key = key;
		this.storeLoc=dataStoreLocation;
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

			Class<?>[] parameterTypes = { String.class, String.class };

			Object[] args = { this.key, this.storeLoc };

			Method methodToInvoke;
			methodToInvoke = classType.getMethod("delete", parameterTypes);
			methodToInvoke.invoke(instance, args);

		} catch (Exception e) {
			isDone = false;
			e.printStackTrace();
		}
		return isDone;
	}
}
