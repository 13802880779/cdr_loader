package CdrUtils;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime; 
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class StrUtils {
	public static DateTimeFormatter DF1;
	public static  DateTimeFormatter DF2;  
	static
	{
		DF1=DateTimeFormat.forPattern("yyyyMMddHHmmssSSS");
		DF2= DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
		//DF1.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		//DF2.setTimeZone(TimeZone.getTimeZone("GMT+8"));
	}
	
	public static void main(String args[]) throws ParseException
	{
		//8108
		long st=System.currentTimeMillis();
		String s="1445562182,2015-10-23 08:49:25.843,460002406341735,17052520720,8613602479832,gz,9493,255,14748,2,8665000233761900,cmwap,180840368,167772332,0,-575579903,-575580043,http://211.136.221.105/XIxZpAfewk+A,102308492592200008331,0430000075561151023084925001,0,1000027184,1380191600,255,200,247,0,0,0,438,510,1,Android-Mms/2.0 (Linux; U; Android 4.4.4; zh-cn; Xiaomi MI 4LTE Build/KTU84P),3,2021,0,2021,8519629";
		String ss[]=split(s,",");
		//System.out.println("==>"+s.length());
			//String s2="\x00\x00\x00\x96";

			//System.out.println(Integer.valueOf(0x96,16));
			
	//	}
	//	long end=System.currentTimeMillis();
		//System.out.println(end-st);
		System.out.println(ss.length+","+StrUtils.datestr2long2(ss[1])+","+ss[3]+","+ss[37]);

			  DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");   
			  DateTime dateTime2 = DF2.parseDateTime("2015-10-22 21:18:10.193");
		//	 System.out.println(dateTime2.toDate());
					  //DateTime.parse("2012-12-21 23:22:45", format);      
	}

	public static String[] split(String line,String delim)
	{
	//	String line="10.190.174.142-----[03/Dec/2011:13:28:11--0800]-GET-/images/filmmediablock/360/GOEMON-NUKI-000159.jpg-HTTP/1.1-200-";
		int pos_s=0;
		int pos_e=0;		
		ArrayList<String> strlist=new ArrayList();
		while((pos_e=line.indexOf(delim, pos_s))!=-1)
		{
			strlist.add(line.substring(pos_s,pos_e));
			pos_s=pos_e+1;
		}
		
		if(pos_s<=line.length())
		{
				strlist.add(line.substring(pos_s,line.length()));
		}
		return strlist.toArray(new String[strlist.size()]);
	}
	
	private static String[] SplitTextAll(String line,String splitText)
	{
	int position1 = line.indexOf(splitText);  

	int length = splitText.length();
	List<String> arr = new ArrayList<String>();

	int positionstart = - length;
	int positionend = position1;

	while (positionend>=0)
	{
	positionend = line.indexOf(splitText, positionstart + length);
	if(positionend==-1)
	{
	break;
	}

	arr.add(line.substring(positionstart + length, positionend));	
	positionstart = positionend;

	}
	arr.add(line.substring(positionstart + length));
	return arr.toArray(new String[arr.size()]);

	}
	
	public static String[] split(StringBuffer line,String delim)
	{
	//	String line="10.190.174.142-----[03/Dec/2011:13:28:11--0800]-GET-/images/filmmediablock/360/GOEMON-NUKI-000159.jpg-HTTP/1.1-200-";
		int pos_s=0;
		int pos_e=0;	

		ArrayList strlist=new ArrayList();
		while((pos_e=line.indexOf(delim, pos_s))!=-1)
		{
			strlist.add(line.substring(pos_s,pos_e));
			pos_s=pos_e+1;
		}
		
		if(pos_s<=line.length())
		{
				strlist.add(line.substring(pos_s,line.length()));
		}
		return (String[]) strlist.toArray(new String[strlist.size()]);
	}
	public static String  long2datestr(long t)
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		Date d=new Date(t);
		return format.format(d);	
	}
	public static long datestr2long(String datestr) throws ParseException
	{
		//SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		Date d=DF1.parseDateTime(datestr).toDate();
		return d.getTime();

	}
	public static long datestr2long2(String datestr) throws ParseException
	{
		//2015-10-21 14:47:17.176
		//SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date d=DF2.parseDateTime(datestr).toDate();
		return d.getTime();

	}
	//public static String 
	
	
	public static String leftPadWithZero(Object l,int len)
	{
		return String.format("%0"+len+"d", l);
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
}