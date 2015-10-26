package CdrParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import CdrLogger.CLogger;
import CdrUtils.StrUtils;

public class RowkeyTester {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//System.out.println(isBlankString("                 "));
		
		
		try
		{
			BufferedReader br=new BufferedReader(new FileReader(new File("/data/glassfish/bak/PS_USERSERVICE/CITY_86020/bak/user_service_CDR_201510211750_00047_20151021#20151021191312#.dat")));
			String line;
			int rNum=350;
			HashMap<Integer,Integer> hm=new HashMap<Integer,Integer>();
			for(int i=0;i<rNum;i++)
				hm.put(i, 0);
			int cc=0;
			
			while((line=br.readLine())!=null)
			{
				String r[]=StrUtils.split(line, ",");
				int salt=generatetRowKeySalt(r,3,rNum);
				if(salt!=-1)
				{
					int counter=hm.get(salt);
					counter++;
					hm.put(salt, counter);
				}
				else
					cc++;
			}
			br.close();
			int biggest=0;
			for(int i=0;i<hm.size();i++)
			{
				System.out.println(i+"====>"+hm.get(i));
				if(hm.get(i)>biggest)
					biggest=hm.get(i);
			}
			System.out.println("biggest====>"+biggest);
	
			
		}catch(Exception e){e.printStackTrace();}

	}

public static HashMap<String,String>testp=new HashMap<String,String>();
	
	public static int generatetRowKeySalt(final String[] r,int idx, int rNum){
		// TODO Auto-generated method stub
		//[4][8][8][4]
		//String r[]=rowItems;
		

		
		try{
		String revMsisdn=r[idx];
		long msisdn;
			
		if(StrUtils.isBlankString(revMsisdn))//如果是空号码，那么生成一个随机的18位到数字
		{
			
			msisdn=(long)(Math.random()*1000000000000000000L);//生成一个随机到18位数字
			revMsisdn=StringUtils.rightPad(String.valueOf(msisdn), 18,"0");
			//补全到16位到长整数
			msisdn=Long.parseLong(revMsisdn);
		}
		else{
		//对小于11位的号码，如固话号码，左补0到11位（正常到msisdn位长)
		 revMsisdn=StringUtils.leftPad((new StringBuffer(r[idx])).reverse().toString(),11,"0");
		 //注意，有可能含有非数字字符,正常返回11位到long数字，当以下异常时，将取字符串hashcode，并返回固定位长的long
		 //1）revMsisdn可能包含字符,2)revMsisdn长度可能超过11位
		 //msisdn最长应该在11-15位间，返回16位右补长字段即可
		 msisdn=getAbsoluteLong(revMsisdn,16);
		}
		
		
		int salt=Math.abs(revMsisdn.hashCode())%rNum;
		//if(salt==142)
		//	System.out.println(r[idx]);
			
	//	if(salt==142)
	//	{
	//		int counter=phone.get(revMsisdn);
	//		phone.put(revMsisdn, counter+1);
	//	}
		
		return salt;
	
		}catch(Exception e){
			//e.printStackTrace();
			
			//CLogger.logStackTrace(e);
			return -1;
		}
		
	}
	public static boolean isBlankString(final String str)
	{
		char chars[]=str.toCharArray();
		if(chars.length==0)
			return true;
		for(char c:chars)
		{
			//System.out.println(c!=' ');
			if(c!=' ')
				return false;
		}
		return true;
	}
	
	
	public static long getAbsoluteLong(String str,int fixLen)
	{
		try{
			return Long.parseLong(str);
		}catch(NumberFormatException e)
		{
			//返回18位的long数字,通过补全str到hashcode
			return Long.parseLong(StringUtils.rightPad(String.valueOf(Math.abs(str.hashCode())), fixLen,"0"));
		}
	}
}
