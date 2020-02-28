package com.filestore.common;

public class StoreException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	public StoreException() {
		
	}
	
	public StoreException(String message) {
		super(message);
	}
	
	public StoreException(Exception exception) {
		super(exception);
	}
	
	public StoreException(Throwable throwable) {
		super(throwable);
	}
}
