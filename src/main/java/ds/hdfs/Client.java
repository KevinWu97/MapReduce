package ds.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {
    public DataNodeInterface dataStub;
    public NameNodeInterface nameStub;

    public Client(){
        // Put stuff here later
    }

    public DataNodeInterface getDataStub(String dataId, String dataIp, int port){
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(dataIp, port);
                return (DataNodeInterface)registry.lookup(dataId);
            }catch(Exception ignored){}
        }
    }

    public NameNodeInterface getNameStub(String nameId, String nameIp, int port){
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(nameId, port);
                return (NameNodeInterface)registry.lookup(nameIp);
            }catch(Exception ignored){}
        }
    }

    public void putFile(String fileName){
        System.out.println("Going to put file " + fileName);
        File file = new File(fileName);
        FileInputStream fileInputStream;
        try{
            fileInputStream = new FileInputStream(file);
        }catch(Exception e){
            System.out.println("File " + fileName + " not found!");
        }
    }

    public void getFile(String fileName){
        System.out.println("Going to get file " + fileName);
        File file = new File(fileName);
        FileOutputStream fileOutputStream;
        try{
            fileOutputStream = new FileOutputStream(file);
        }catch(Exception e){
            System.out.println("File " + fileName + " not found!");
        }
    }

    public void list(){

    }

    public static void main(String[] args){

    }
}
