package cn.tao.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HDFSClientDemo {
	FileSystem fs=null;
	Configuration conf=null;
	
	/*
	 * 初始化操作
	 */
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException{
		conf=new Configuration();
		conf.set("dfs.replication", "5");
		
		//获取一个文件系统操作的客户端实例对象
		fs=FileSystem.get(conf);
		//可以直接传入uri和用户身份
		fs=FileSystem.get(new URI("hdfs://10.108.21.2:9000"),conf,"root");
	}
	
	/*
	 * 上传操作
	 */
	@Test
	public void testUpload() throws IllegalArgumentException, IOException{
		fs.copyFromLocalFile(new Path("D:/a.avi"), new Path("/"));
	    fs.close();
	}
	
	/*
	 * 下载操作
	 */
	@Test
	public void testDownload() throws Exception, IOException{
		fs.copyToLocalFile(new Path("/a.avi"), new Path("D:/mysql"));
		fs.close();
	}
	
	/*
	 * 打印参数
	 */
	@Test
	public void testConf(){
		Iterator<Entry<String, String>> iterator = conf.iterator();
	    while(iterator.hasNext()){
	    	Entry<String,String> ent=iterator.next();
	    	System.out.println(ent.getKey()+"--"+ent.getValue());
	    }
	}
	
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException{
		boolean b = fs.mkdirs(new Path("/testmkdir/aaa/bbb"));
		System.out.println(b);
		fs.close();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testDelete() throws IllegalArgumentException, IOException{
		boolean b = fs.delete(new Path("/testmkdir/aaa"));
		System.out.println(b);
		fs.close();
	}
	
	@Test
	public void testLs() throws FileNotFoundException, IllegalArgumentException, IOException{
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()){
		    LocatedFileStatus fileStatus = listFiles.next();
		    System.out.println("blocksize: "+fileStatus.getBlockSize());
		    System.out.println("owner: "+fileStatus.getOwner());
		    System.out.println("replication: "+fileStatus.getReplication());
		    System.out.println("Permission: "+fileStatus.getPermission());
		    System.out.println("name: "+fileStatus.getPath().getName());
		    System.out.println("----------------------------");
		    fs.close();
		}
		fs.close();
	}
	
	@Test
	public void testLs2() throws FileNotFoundException, IllegalArgumentException, IOException{
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus f:listStatus){
			System.out.println(f);
		}
		fs.close();
	}
}
