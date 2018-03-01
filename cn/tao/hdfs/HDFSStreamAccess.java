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
 * �����ķ�ʽ����HDFS�ϵ��ļ�
 * ����ʵ�ֶ�ȡָ��ƫ������Χ������
 */
public class HDFSStreamAccess {
	FileSystem fs=null;
	Configuration conf=null;
	
	/*
	 * ��ʼ������
	 */
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException{
		conf=new Configuration();
		conf.set("dfs.replication", "5");
		
		//��ȡһ���ļ�ϵͳ�����Ŀͻ���ʵ������
		//fs=FileSystem.get(conf);
		//����ֱ�Ӵ���uri���û����
		fs=FileSystem.get(new URI("hdfs://10.108.21.2:9000"),conf,"root");
	}
	
	/*
	 * ͨ�����ķ�ʽ�������ϴ���HDFS
	 */
	@Test
	public void testUpload() throws Exception, IOException{
		FSDataOutputStream outputStream = fs.create(new Path("/wc/input/a.txt"), true);
	    FileInputStream inputStream = new FileInputStream("D:/demo.txt");
	    IOUtils.copy(inputStream, outputStream);
	}
	
	/*
	 * ͨ�����ķ�ʽ��HDFS��������
	 */
	@Test
	public void testDownload() throws Exception, IOException{
		FSDataInputStream inputStream = fs.open(new Path("/wc/input/a.txt"));
	    FileOutputStream outputStream=new FileOutputStream("D:/mysql/k.txt");
	    
	    IOUtils.copy(inputStream, outputStream);
	}
	
	/*
	 * ͨ�����ķ�ʽ��ȡָ����Χ����
	 */
	@Test
	public void testRandomAccess() throws Exception, IOException{
		FSDataInputStream inputStream = fs.open(new Path("/wc/input/a.txt"));
	    
	    FileOutputStream outputStream=new FileOutputStream("d:/mysql/c.txt");
	    IOUtils.copyLarge(inputStream, outputStream, 10, 20);
	    IOUtils.copy(inputStream,System.out);
	}
}
