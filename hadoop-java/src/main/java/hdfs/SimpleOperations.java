package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SimpleOperations {
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

    public void createDirectory() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.0.2.15:9000//");

        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path("hdfs://10.0.2.15:9000//rdfTestDirectory");

        fs.mkdirs(dir);
    }

    public void saveCollectionAsFile() throws IOException {
        List<String> names = Arrays.asList("marek", "roman", "ania", "kasia", "ronald");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.122.54:19000//");

        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path("hdfs://192.168.122.54:19000//rdfTestDirectory/rdfTestFile");

        FSDataOutputStream file = fs.create(filePath);
        file.write(String.join(" ", names).replace(" ", "\n").getBytes());
    }

    public void createDirAndMoveFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.122.54:19000//");

        FileSystem fs = FileSystem.get(conf);
        String fileName = "rdfTestFile";
        String mainDirPath = "hdfs://192.168.122.54:19000//rdfTestDirectory/";
        String newDirectorPath = "hdfs://192.168.122.54:19000//rdfTestDirectory/littleDirInside/";

        fs.mkdirs(new Path(newDirectorPath));
        FileUtil.copy(fs, new Path("sourcePath"), fs, new Path("destinationPath"), true, conf);
    }
}
