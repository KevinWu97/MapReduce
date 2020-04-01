package ds.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigureHDFS {
    public static void main(String[] args){
        Properties props = new Properties();
        File file = new File("hdfs.properties");
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file);

            int blockSize = 64000000;
            int repFactor = 3;

            if(args.length == 2){
                blockSize = Integer.parseInt(args[0]);
                repFactor = Integer.parseInt(args[1]);
            }else if(args.length == 1){
                // Illegal number of arguments
                throw new IOException();
            }

            props.setProperty("block_size", String.valueOf(blockSize));
            props.setProperty("rep_factor", String.valueOf(repFactor));
            props.store(fileOutputStream, null);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
