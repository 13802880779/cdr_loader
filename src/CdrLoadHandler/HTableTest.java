package CdrLoadHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import CdrUtils.HBaseUtils;

public class HTableTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		int count=4;
		
		HTableInterface htables[]=new HTableInterface[count];
		for(int i=0; i<count; i++)
		{	
			System.out.println("==>"+"test"+i);
			htables[i]=HBaseUtils.getConnection().getTable("test"+i);
			htables[i].setAutoFlush(false);
			htables[i].setWriteBufferSize(1024*1024*8);
			Put p=new Put(Bytes.toBytes("test"+i*2));
			p.add(Bytes.toBytes("C"), Bytes.toBytes("D"), Bytes.toBytes("test"+i));
			List<Put> list=new ArrayList<Put>();
			list.add(p);
			
			htables[i].put(list);
			
		
		}
		for(int i=0; i<count; i++)
		{
			//htables[i].flushCommits();
			htables[i].close();
		}
		
		System.out.println("done!");
		
		
		

	}

}
