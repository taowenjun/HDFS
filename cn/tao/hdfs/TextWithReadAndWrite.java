package cn.tao.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class TextWithReadAndWrite {

	public static void main(String[] args) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path("ample.txt");
		Text text = new Text("helloworld");
		text.write(fs.create(path));
		Text str = new Text();
		str.readFields(new FSDataInputStream(fs.open(path)));
		System.out.println(str.toString());
	}

}
