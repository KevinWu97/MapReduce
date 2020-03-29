package ds.hdfs;

import proto.ProtoHDFS;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DataNode implements DataNodeInterface {
    // This data structure allows thread safe access to the blocks of this specific data node
    protected ConcurrentHashMap<String, Boolean> requestsFulfilled;
    protected ConcurrentHashMap<String, ProtoHDFS.BlockMeta> blockMetas;
    protected String dataId;
    protected String dataIp;
    protected int port;

    @Override
    public byte[] readBlock(byte[] inp) throws IOException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

        // A request sent to readBlocks should only contain a block list consisting of a single block
        List<ProtoHDFS.Block> requestBlockList = request.getBlockList();
        LinkedList<ProtoHDFS.Block> blockList = new LinkedList<>(requestBlockList);

        ProtoHDFS.Block block = blockList.pop();
        ProtoHDFS.BlockMeta blockMeta = block.getBlockMeta();

        String fileName = blockMeta.getFileName();
        int blockNumber = blockMeta.getBlockNumber();
        int repNumber = blockMeta.getRepNumber();
        String blockName = fileName + "_" + blockNumber + "_" + repNumber;

        if(this.blockMetas.containsKey(blockName)){
            byte[] blockBytes = Files.readAllBytes(Paths.get(blockName));
            String blockContents = new String(blockBytes);

            ProtoHDFS.Block.Builder blockBuilder = ProtoHDFS.Block.newBuilder();
            blockBuilder.setBlockMeta(this.blockMetas.get(blockName));
            blockBuilder.setBlockContents(blockContents);
            ProtoHDFS.Block responseBlock = blockBuilder.build();
            blockBuilder.clear();

            ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
            responseBuilder.setBlock(responseBlock);
            responseBuilder.setErrorMessage(String.format("Block %1$d replication %2$d for %3$s read success",
                    blockNumber, repNumber, fileName));
            ProtoHDFS.Response response = responseBuilder.buildPartial();
            responseBuilder.clear();

            return response.toByteArray();
        }else{
            ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.FAILURE);
            responseBuilder.setErrorMessage(String.format("Block %1$d replication %2$d for %3$s does not exist",
                    blockNumber, repNumber, fileName));
            ProtoHDFS.Response response = responseBuilder.buildPartial();
            responseBuilder.clear();

            return response.toByteArray();
        }
    }

    @Override
    public byte[] writeBlock(byte[] inp) throws IOException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

        // Make the replication factor configurable later
        int repFactor = 3;
        List<ProtoHDFS.Block> requestBlockList = request.getBlockList();
        LinkedList<ProtoHDFS.Block> blockList = new LinkedList<>(requestBlockList);

        ProtoHDFS.Block block = blockList.pop();
        ProtoHDFS.BlockMeta blockMeta = block.getBlockMeta();
        String blockContents = block.getBlockContents();

        String fileName = blockMeta.getFileName();
        int blockNumber = blockMeta.getBlockNumber();
        int repNumber = blockMeta.getRepNumber();

        if(repNumber < repFactor){
            // Send a 'request' object to the next data node to replicate block on another data node
            // Still need to figure out how to access other data nodes from a data node
        }

        String blockName = fileName + "_" + blockNumber + "_" + repNumber;
        File file = new File(blockName);
        FileOutputStream fileOutputStream;
        if(file.exists() || file.createNewFile()){
            fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(blockContents.getBytes());
            fileOutputStream.flush();
            fileOutputStream.close();
        }

        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
        responseBuilder.setErrorMessage(String.format("Block %1$d replication %2$d for %3$s write success",
                blockNumber, repNumber, fileName));
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();
        return response.toByteArray();
    }

    // This method binds the Data Node to the server so the client can access it and use its services (methods)
    public void bindServer(String dataId, String dataIp, int dataPort){
        try{
            // This is the stub which will be used to remotely invoke methods on another Data Node
            // Initial value of the port is set to 0
            DataNodeInterface dataNodeStub = (DataNodeInterface) UnicastRemoteObject.exportObject(this, 0);

            // This sets the IP address of this particular Data Node instance
            System.setProperty("java.rmi.server.hostname", dataIp);

            // This gets reference to remote object registry located at the specified port
            Registry registry = LocateRegistry.getRegistry(dataPort);

            // This rebinds the Data Node to the remote object registry at the specified port in the previous step
            // Uses the values of id (or 'name') of the Data Node and a Remote which is the stub for this Data node
            registry.rebind(dataId, dataNodeStub);

            System.out.println("\n Data Node connected to RMI registry \n");
        }catch(Exception e){
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
        }
    }

    public DataNodeInterface getDNStub(String dataId, String dataIp, int dataPort) {
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(dataIp, dataPort);
                DataNodeInterface dataNodeStub = (DataNodeInterface)registry.lookup(dataId);
                System.out.println("\n Data Node Found! Replicating to Data Node \n");
                return dataNodeStub;
            }catch(RemoteException | NotBoundException e){
                System.out.println("\n Searching for Data Node ... \n");
            }finally{
                System.out.println("timed out?!?!??!");
            }

        }
    }

    // This method finds the Name Node and returns a stub (Remote to the Name Node) with which the Data Node
    // could use to invoke functions on the Name Node
    public NameNodeInterface getNNStub(String id, String ip, int port){
        while(true){
            try{
                // This gets the remote object registry at the specified port
                Registry registry = LocateRegistry.getRegistry(ip, port);
                // This gets the Remote to the Name Node using the ID of the Name Node
                NameNodeInterface nameNodeStub = (NameNodeInterface)registry.lookup(id);
                System.out.println("\n Name Node Found! \n");
                return nameNodeStub;
            }catch(Exception e){
                System.out.println("\n Searching for Name Node ... \n");
            }
        }
    }

    public static void main(String[] args){

    }
}
