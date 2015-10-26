package CdrMonitor;

import org.apache.hadoop.hbase.util.Bytes;

import CdrUtils.StrUtils;

public class MonitorObject {
	private String mObjName;
	private long mObjTimeStamp;
	private long mObjValue;
	private byte[] key;
	private byte[] value;
	public MonitorObject(String obj,long ts, long value)
	{
		this.mObjName=obj;
		this.mObjTimeStamp=ts;
		this.mObjValue=value;
	}
	public String getName()
	{
		return this.mObjName;
	}
	public long getTimeStamp()
	{
		return this.mObjTimeStamp;
	}
	public long getValue1()
	{
		return this.mObjValue;
	}
	public byte[] getKey()
	{
		String objName=StrUtils.leftPadWithZero(this.mObjName, 56);
		return Bytes.add(Bytes.toBytes(objName), Bytes.toBytes(this.mObjTimeStamp));
	}
	public byte[] getValue()
	{
		return Bytes.toBytes(this.mObjValue);
	}
	
}
