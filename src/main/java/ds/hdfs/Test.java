package ds.hdfs;

import java.io.*;

public class Test {
    public static void main(String[] args){
        File file = new File("randomFile.txt");
        File out = new File("outFile.txt");
        int readBytes = 0;
        try {
            byte[] b = new byte[10];
            if((file.exists() || file.createNewFile() && (out.exists() || out.createNewFile()))){
                FileInputStream fileInputStream = new FileInputStream(file);
                FileOutputStream fileOutputStream = new FileOutputStream(out);
                while((readBytes = fileInputStream.read(b)) != -1){
                    fileOutputStream.write(b, 0, readBytes);
                    fileOutputStream.write("cool beans".getBytes());
                }
                fileInputStream.close();
                fileOutputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
