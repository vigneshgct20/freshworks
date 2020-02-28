package com.filestore.monitor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class DataStoreMonitoringProcessDriver
{
	private static Map<String, ScheduledThreadPoolExecutor> executors = new HashMap<>();

	private DataStoreMonitoringProcessDriver() {
		
	}
	
	public static void init(String storeLocation) {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
		executor.scheduleWithFixedDelay(new DataStoreMonitoringProcess(storeLocation), 0, 1, TimeUnit.SECONDS);
		executors.put(DataStoreMonitoringProcess.class.getSimpleName(), executor);
	}
}
