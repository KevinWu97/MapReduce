package ds.hdfs;

import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeInterface extends Remote {

    /* Method to read data from any block given block-number */
    byte[] readBlock(byte[] inp) throws IOException;

    /* Method to write data to a specific block */
    byte[] writeBlock(byte[] inp) throws IOException, NotBoundException;
}
