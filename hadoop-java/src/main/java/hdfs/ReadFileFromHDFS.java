package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

public class ReadFileFromHDFS {
    public void read() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.122.54:19000//");

        FileSystem fs = FileSystem.get(conf);
        Path inFile = new Path("hdfs://192.168.122.54:19000//fileWithPath");

        FSDataInputStream in = fs.open(inFile);
        OutputStream out = System.out;
        byte buffer[] = new byte[256];
        int bytesRead = 0;
        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
        }

        in.close();
        out.close();
    }
}
