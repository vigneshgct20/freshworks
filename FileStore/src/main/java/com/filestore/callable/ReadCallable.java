package com.filestore.callable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.codehaus.jettison.json.JSONObject;

public class ReadCallable implements Callable<JSONObject>
{

	private String key = null;

	private String storeLoc = null;

	public ReadCallable(String key, String string) {
		this.key = key;
		this.storeLoc = storeLoc;
	}

	@Override
	public JSONObject call() throws Exception {
		boolean isDone = true;
		JSONObject json = new JSONObject();
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
			methodToInvoke = classType.getMethod("readFromStore", parameterTypes);
			json = (JSONObject) methodToInvoke.invoke(instance, args);

		} catch (Exception e) {
			isDone = false;
			e.printStackTrace();
		}
		return json;
	}

}
