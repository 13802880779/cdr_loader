package CdrMonitor;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import CdrUtils.StrUtils;

public class MonitorObject {
	private String mObjName;
	private String mObjKpiName;
	private long mObjTimeStamp;
	//private long mObjValue;
	private byte[] key;
	private byte[] value;
	public MonitorObject(String obj,String kpi,long ts, long value)
	{
		this.mObjName=obj;
		this.mObjTimeStamp=ts;
		//this.mObjValue=value;
		this.value=Bytes.toBytes(value);
		this.mObjKpiName=kpi;
	}
	public MonitorObject(String obj,String kpi,long ts, String value)
	{
		this.mObjName=obj;
		this.mObjTimeStamp=ts;
		//this.mObjValue=value;
		this.value=Bytes.toBytes(value);
		this.mObjKpiName=kpi;
	}
	public String getName()
	{
		return this.mObjName;
	}
	public long getTimeStamp()
	{
		return this.mObjTimeStamp;
	}

	public byte[] getKey()
	{
		String objName=StringUtils.leftPad(this.mObjName+":"+this.mObjKpiName, 56, "0");
		return Bytes.add(Bytes.toBytes(objName), Bytes.toBytes(this.mObjTimeStamp));
	}
	public byte[] getValue()
	{
		return this.value;
	}
	
}
