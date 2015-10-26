package CdrLoadHandler;


import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import CdrConfiguration.CConf;
import CdrParser.CdrIndexParser;
import CdrParser.HWCdrParser;
import CdrParser.IndexObj;
import CdrUtils.StrUtils;
import CdrUtils.HBaseUtils;
public class CdrQuery {

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CdrQuery q =new CdrQuery("AIU_HO_CDR");
		String idxVal[]=new String[]{"860308024609949"};
		String idxName="IMEI";
		int idxRnum=10;
		//HWCdrParser p=new HWCdrParser();
		CdrIndexParser sip=new CdrIndexParser(CConf.getCdrIndexConfiguration("hw", "aiu-ho"));
		
		//CdrQuery
		
		try {
			//1435701598605
			long starttime=1404884000000L;
			long endtime=1404884999999L;
			//System.out.println("ts:"+StrUtils.long2datestr(1404884853000L)+"\ns:"+starttime+"\ne:"+endtime);
			//	byte[] getIndexScanKey(CdrSecIdxParser csip, String idxName,String[] idx,int rNum,long ts)

			byte[] startkey= getIndexScanKey(sip,idxName,idxVal,idxRnum,starttime);
			byte[] endkey= getIndexScanKey(sip,idxName,idxVal,idxRnum,endtime);
			ArrayList<byte[]> al=q.scanIndex("AIU_HO_CDR_INDEX", startkey, endkey);
			
			ArrayList<Get> glist=new ArrayList<Get>();
			for(byte[] rk:al)
			{
				Get g=new Get(rk);
				g.addColumn(Bytes.toBytes("C"), Bytes.toBytes("D"));
				glist.add(g);
				
			}
			batchGet("AIU_HO_CDR",glist);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally
		{
			try {
				q.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	String tn;
	HTableInterface htable;
	public CdrQuery(String tableName)
	{
		tn=tableName;

	}
	
	public ArrayList<byte[]> scanIndex(String tn, byte[] startkey,byte[] endkey) throws IOException
	{
		HConnection conn=HBaseUtils.getConnection();

		htable=conn.getTable(tn);

		Scan scan=new Scan();
			
		ArrayList result=new ArrayList();
	
			
		scan.setStartRow(startkey);
		scan.setStopRow(endkey);
		scan.setCaching(1000);
		ResultScanner rs=htable.getScanner(scan);
		
		for(Result r:rs)
		{
			//System.out.println("get rowkey!");
			//r.get
			byte[] rk=Arrays.copyOfRange(r.getRow(), 24, r.getRow().length);
			result.add(rk);
		}
		htable.close();
		
		return result;
		
		
	}
	
	public static void batchGet(String tn, ArrayList<Get> glist) throws IOException	
	{
		HConnection conn=HBaseUtils.getConnection();
		HTableInterface ht=conn.getTable(tn);
		
		Result rs[]=ht.get(glist);
		for(Result r:rs)
		{
			System.out.println(Bytes.toString(r.getValue(Bytes.toBytes("C"), Bytes.toBytes("D"))));
		}
		
		ht.close();
		
	}
	

	
	public ArrayList<String> scan(String msisdn,long starttime,long endtime) throws IOException
	{
		HConnection conn=HBaseUtils.getConnection();

		htable=conn.getTable(this.tn);

		Scan scan=new Scan();
			
		ArrayList result=new ArrayList();
	
		String revMsisdn=StringUtils.leftPad((new StringBuffer(msisdn)).reverse().toString(),11,"0");
		
		int RNum=Bytes.toInt((HBaseUtils.getTableMeta(tn, "RegionsNum")));
		
		int salt=Math.abs(revMsisdn.hashCode())%RNum;	//numPartion	

		byte[] startkey=Bytes.add(Bytes.toBytes(salt), Bytes.toBytes(Long.parseLong(revMsisdn)),Bytes.toBytes(starttime));
		byte[] endkey=Bytes.add(Bytes.toBytes(salt), Bytes.toBytes(Long.parseLong(revMsisdn)),Bytes.toBytes(endtime));

		
		scan.setStartRow(startkey);
		scan.setStopRow(endkey);
		scan.setCaching(1000);
		ResultScanner rs=htable.getScanner(scan);
		
		for(Result r:rs)
		{

			String cdrline=Bytes.toString(r.getValue(Bytes.toBytes("C"), Bytes.toBytes("D")));
			result.add(cdrline);
		}
		return result;
		
		
	}

	static byte[] getIndexScanKey(CdrIndexParser csip, String idxName,String[] idx,int rNum,long ts)
	{
	
			IndexObj obj=csip.getIdxObjByName(idxName);
			if(obj==null||idx.length!=obj.ctnNum)
				return null;
			
			StringBuffer info=new StringBuffer("");
			byte[] rk1=new byte[12];//保存索引值
			
			String content[]=new String[obj.ctnNum];
			StringBuffer sb=new StringBuffer();//保存combine类型索引的合并字符串
			
			for(int i=0; i<idx.length;i++)
			{
				if(idx[i].length()>obj.ctnLen[i])
					content[i]=content[i].substring(0, obj.ctnLen[i]);
				else					
					content[i]=StringUtils.leftPad(idx[i], obj.ctnLen[i], "0");					
				sb.append(content[i]);
			}
			
			int salt=Math.abs(sb.toString().hashCode())%rNum;			
			info.append("salt=>"+salt);
			

			int offset=0;
			offset=Bytes.putBytes(rk1, offset, obj.head.getBytes(), 0, obj.head.length());
			
			info.append(" index_name=>"+obj.head);
			info.append(" index_value=>");
			for(int i=0;i<content.length; i++)
			{
				//System.out.println("==>"+content[i].hashCode());
				switch(obj.dataType[i]){
				case "INT":
					offset=Bytes.putInt(rk1, offset, Integer.parseInt(content[i]));
					info.append(" int:"+Integer.parseInt(content[i]));
				//	System.out.println(offset+":INT==>"+Integer.parseInt(content[i]));
					break;
				case "LONG":
					offset=Bytes.putLong(rk1, offset, Long.parseLong(content[i]));
					info.append(" long:"+Long.parseLong(content[i]));
				//	System.out.println(offset+":LONG==>"+Long.parseLong(content[i]));
					break;
				case "SHORT":
					offset=Bytes.putShort(rk1, offset, Short.parseShort(content[i]));
					info.append(" short:"+Short.parseShort(content[i]));
				//	System.out.println(offset+":SHORT==>"+Short.parseShort(content[i]));
					break;
				case "STR":
				default:
					offset=Bytes.putBytes(rk1, offset, content[i].getBytes(), 0, obj.ctnLen[i]);			
					info.append(" str:"+content[i]);
				//	System.out.println(offset+":STR==>"+new String(Bytes.copy(content[i].getBytes(),0,obj.ctnLen[i])));
				}
			}
		
			info.append(" ts==>"+ts);
			System.out.println(info.toString());
		return Bytes.add(Bytes.toBytes(salt),
				rk1,
				Bytes.toBytes(ts));
	}
	
	public void close() throws IOException
	{
		htable.close();
	}

}
