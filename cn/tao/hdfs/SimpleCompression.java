package cn.tao.hdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

public class SimpleCompression {

	public static void main(String[] args) throws IOException {
		//生成环境变量
		Configuration configuration=new Configuration();
		//创建文件系统实例
		FileSystem fs=FileSystem.get(configuration);
		//设置输出路径
		Path outputPath=new Path("helloworld.gz");
		//创建输出路径
		OutputStream os=fs.create(outputPath);
		//生成压缩格式实例
		CompressionCodec codec=new GzipCodec();
		//将字符串转换为字符数组
		byte[] buff="hello world".getBytes();
		//创建压缩环境地址
		CompressionOutputStream cos=codec.createOutputStream(os);
		//写入数据
		cos.write(buff);
		//关闭输出流
		cos.close();

	}

}
