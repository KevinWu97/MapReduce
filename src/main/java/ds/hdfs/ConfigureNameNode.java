package ds.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

public class ConfigureNameNode {
    public static void main(String[] args){
        Properties props = new Properties();
        File file = new File("namenode.properties");
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file);

            InetAddress inetAddress = InetAddress.getLocalHost();
            String nodeName = UUID.randomUUID().toString();
            String nodeIp = inetAddress.getHostAddress();
            String port;
            if(args.length == 0){
                // If no port number is passed, default value of port is 1099
                port = "1099";
            }else{
                port = args[0];
            }

            props.setProperty("server_name", nodeName);
            props.setProperty("server_ip", nodeIp);
            props.setProperty("server_port", port);
            props.store(fileOutputStream, null);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
