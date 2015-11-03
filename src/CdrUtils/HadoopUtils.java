package CdrUtils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import CdrLogger.CLogger;

public class HadoopUtils {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	//	uploadFile2HDFS("/root/apache-maven-3.3.3.tar.gz","/tmp/apache-maven-3.3.3.tar.gz");
	//	deleteHDFSFile("/tmp/test/apache-maven-3.3.3.tar.gz");
	}
	
	private static int DFS_REPLICATION=2;
	public static void setDfsReplication(int r)
	{
		DFS_REPLICATION=r;
	}
	public static int uploadFile2HDFS(String s,String d) throws IOException
	{
		Configuration config = new Configuration();
		config.setInt("dfs.replication", DFS_REPLICATION);
        FileSystem hdfs=null;
   //     try{
		hdfs = FileSystem.get(config);

        Path src = new Path(s);
        Path dst = new Path(d);
        Path dst_parent=dst.getParent();
        if(!hdfs.exists(dst_parent))
        	hdfs.mkdirs(dst_parent);
        
        hdfs.copyFromLocalFile(src, dst);
  	
	/*	}finally
		{	try {
			if(hdfs!=null)
					hdfs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}*/
        
		return 1;
	}

	public static boolean deleteHDFSFile(String dest) throws IOException
	{
		Configuration config = new Configuration();
        FileSystem hdfs=null;
		//try {
			hdfs = FileSystem.get(config);
        Path path = new Path(dest);
        if (hdfs.exists(path)) {  
        	hdfs.delete(path, true);        	
        	return true;
        }
        else       
        	return false;
 		/*} finally
		{
			 try {
				 if(hdfs!=null)
					 hdfs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
        
	}
}
