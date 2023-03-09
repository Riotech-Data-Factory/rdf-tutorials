import hdfs.SimpleOperations;

import java.io.IOException;

public class App {
    static public void main(String[] args) throws IOException {
        System.out.println("Hey Hay Hello!");
        new SimpleOperations().createDirectory();
    }
}
