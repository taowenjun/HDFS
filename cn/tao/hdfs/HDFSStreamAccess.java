package cn.tao.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/*
 * 用流的方式操作HDFS上的文件
 * 可以实现读取指定偏移量范围的数据
 */
public class HDFSStreamAccess {
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
		//fs=FileSystem.get(conf);
		//可以直接传入uri和用户身份
		fs=FileSystem.get(new URI("hdfs://10.108.21.2:9000"),conf,"root");
	}
	
	/*
	 * 通过流的方式将数据上传到HDFS
	 */
	@Test
	public void testUpload() throws Exception, IOException{
		FSDataOutputStream outputStream = fs.create(new Path("/wc/input/a.txt"), true);
	    FileInputStream inputStream = new FileInputStream("D:/demo.txt");
	    IOUtils.copy(inputStream, outputStream);
	}
	
	/*
	 * 通过流的方式从HDFS下在数据
	 */
	@Test
	public void testDownload() throws Exception, IOException{
		FSDataInputStream inputStream = fs.open(new Path("/wc/input/a.txt"));
	    FileOutputStream outputStream=new FileOutputStream("D:/mysql/k.txt");
	    
	    IOUtils.copy(inputStream, outputStream);
	}
	
	/*
	 * 通过流的方式获取指定范围数据
	 */
	@Test
	public void testRandomAccess() throws Exception, IOException{
		FSDataInputStream inputStream = fs.open(new Path("/wc/input/a.txt"));
	    
	    FileOutputStream outputStream=new FileOutputStream("d:/mysql/c.txt");
	    IOUtils.copyLarge(inputStream, outputStream, 10, 20);
	    IOUtils.copy(inputStream,System.out);
	}
}
