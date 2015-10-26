package CdrFileHandler;

import java.io.File;
import java.io.IOException;

public class FileWatcherTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		newFileThread ft=new newFileThread();
		ft.start();
	}
	
	public static class newFileThread extends Thread
	{
		public void run()
		{
			int counter=0;
			while(true)
			{
				counter++;
				if(counter>200)
					break;
				String path="/data/glassfish/bak/CS_AIU_MTC/CITY_86020/bak/";
				for(int i=0;i<1000;i++)
				{
					File f=new File(path+"aiu-mtc-cdr-201504291301-00001_20150429#"+i+".dat");
					if(f.exists())
						f.delete();
					else
						try {
							f.createNewFile();
							//System.out.println("create file "+f.getAbsolutePath());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
