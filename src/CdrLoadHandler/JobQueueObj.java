package CdrLoadHandler;

public class JobQueueObj {
	public String JobName;
	public long JobAddTime;
	public JobQueueObj(String jn,long jt)
	{
		this.JobAddTime=jt;
		this.JobName=jn;
	}

}
