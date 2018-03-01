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
		//���ɻ�������
		Configuration configuration=new Configuration();
		//�����ļ�ϵͳʵ��
		FileSystem fs=FileSystem.get(configuration);
		//�������·��
		Path outputPath=new Path("helloworld.gz");
		//�������·��
		OutputStream os=fs.create(outputPath);
		//����ѹ����ʽʵ��
		CompressionCodec codec=new GzipCodec();
		//���ַ���ת��Ϊ�ַ�����
		byte[] buff="hello world".getBytes();
		//����ѹ��������ַ
		CompressionOutputStream cos=codec.createOutputStream(os);
		//д������
		cos.write(buff);
		//�ر������
		cos.close();

	}

}
