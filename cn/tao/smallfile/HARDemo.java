package cn.tao.smallfile;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
/*
 * ���ܣ����ʴ���С�ļ��ϲ��ɵ�HAR�ļ��е�ָ���ļ�
 */
public class HARDemo {

	public static void main(String[] args) throws IOException, URISyntaxException {
		long point1=System.currentTimeMillis();
		//configuration��Ϣ
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.108.21.2:9000");
		long point2=System.currentTimeMillis();
		//����HARFileSystem
		HarFileSystem fs = new HarFileSystem();
		long point3=System.currentTimeMillis();
		//��ʼ��HAR�ļ�
		fs.initialize(new URI("har:///flume/hello.har"), conf);
		long point4=System.currentTimeMillis();
		
		//FileStatus[] listStatus=fs.listStatus(new Path("har:///flume/hello.har"));
	    //File file=new File("D:\\output.txt");
	   // OutputStream out=new FileOutputStream(file);
		//for(FileStatus fileStatus:listStatus){
		//	Path path = fileStatus.getPath();
		//	FSDataInputStream open = fs.open(path);
		//	String str=open.readLine();
		//	out.write(str.getBytes());
		//	System.out.println(str);
		//	System.out.println(fileStatus.getPath().toString());
		//}
		//out.close();
		
		long point5=System.currentTimeMillis();
		//��ִ���ļ�
		Path path = new Path("har:/flume/hello.har/events-.1502977057500");
		FSDataInputStream open = fs.open(path);
		//��ȡ�ļ�����
		String str=open.readLine();
		System.out.println(str);
		fs.close();
		long point6=System.currentTimeMillis();
		System.out.println("Har��ȡָ���ļ�");
		System.out.println("conf:"+(point2-point1));
	    System.out.println("create fs:"+(point3-point2));
	    System.out.println("initialize fs:"+(point4-point3));
	   // System.out.println("read:"+(point5-point4));
	    System.out.println("read a file:"+(point6-point5));
	    System.out.println("total:"+(point6-point1));
	}
}
