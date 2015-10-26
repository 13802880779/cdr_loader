package CdrExceptions;

import CdrLogger.CLogger;

public class FirmNotFoundException extends Exception{
	public static void main(String args[])
	{
		FirmNotFoundException e=new FirmNotFoundException("hello world!");
		System.out.println(e.toString());
		CLogger.logStackTrace(e);
	}
	
	public FirmNotFoundException(String msg)
	{
		super(msg);
	}

}
