package ds.hdfs;

import proto.ProtoHDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DataNode extends UnicastRemoteObject implements DataNodeInterface {
    // This data structure allows thread safe access to the blocks of this specific data node
    protected ConcurrentHashMap<String, Boolean> requestsFulfilled;
    protected ConcurrentHashMap<String, ProtoHDFS.BlockMeta> blockMetas;

    protected String dataId;
    protected String dataIp;
    protected int dataPort;

    protected DataNode(String dataId, String dataIp) throws RemoteException {
        super();
        this.dataId = dataId;
        this.dataIp = dataIp;
        this.dataPort = 1099;
        this.requestsFulfilled = new ConcurrentHashMap<>();
        this.blockMetas = new ConcurrentHashMap<>();
    }

    protected DataNode(String dataId, String dataIp, int port) throws RemoteException {
        super(port);
        this.dataId = dataId;
        this.dataIp = dataIp;
        this.dataPort = port;
        this.requestsFulfilled = new ConcurrentHashMap<>();
        this.blockMetas = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] readBlock(byte[] inp) throws IOException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

        // If request has never been made before or hasn't been fulfilled, read the block
        // If the request has been fulfilled already, just do the operation again since it's just a read operation
        if(!this.requestsFulfilled.containsKey(requestId) || !this.requestsFulfilled.get(requestId)){
            this.requestsFulfilled.putIfAbsent(requestId, false);
        }

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

            this.requestsFulfilled.replace(requestId, true);
            return response.toByteArray();
        }else{
            ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
            responseBuilder.setResponseId(requestId);
            responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.FAILURE);
            responseBuilder.setErrorMessage(String.format("Block %1$d replication %2$d for %3$s does not exist",
                    blockNumber, repNumber, fileName));
            ProtoHDFS.Response response = responseBuilder.buildPartial();
            responseBuilder.clear();

            this.requestsFulfilled.replace(requestId, true);
            return response.toByteArray();
        }
    }

    @Override
    public byte[] writeBlock(byte[] inp) throws IOException, NotBoundException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();
        ProtoHDFS.Request.RequestType requestType = request.getRequestType();
        // If request has never been made before or hasn't been fulfilled, read the block
        // If the request has been fulfilled already, just do the operation again since it's just a read operation
        if(!this.requestsFulfilled.containsKey(requestId) || !this.requestsFulfilled.get(requestId)){
            this.requestsFulfilled.putIfAbsent(requestId, false);
        }

        // Make the replication factor configurable later
        Properties hdfsProp = new Properties();
        File hdfsPropFile = new File("hdfs.properties");
        FileInputStream hdfsPropInputStream = new FileInputStream(hdfsPropFile);
        hdfsProp.load(hdfsPropInputStream);

        int repFactor = Integer.parseInt(hdfsProp.getProperty("rep_factor", "3"));
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
            // if the number of replicas is not enough
            ProtoHDFS.Request.Builder requestBuilder = ProtoHDFS.Request.newBuilder();
            String replicateRequestId = UUID.randomUUID().toString();
            this.requestsFulfilled.putIfAbsent(replicateRequestId, false);

            requestBuilder.setRequestId(replicateRequestId);
            requestBuilder.setRequestType(requestType);
            requestBuilder.addAllBlock(blockList);
            ProtoHDFS.Request replicateRequest = requestBuilder.buildPartial();
            requestBuilder.clear();

            String dataNodeId = blockMeta.getDataId();
            String dataNodeIp = blockMeta.getDataIp();
            int dataPort = blockMeta.getPort();
            DataNodeInterface dataStub = getDNStub(dataNodeId, dataNodeIp, dataPort);

            byte[] replicateResponseBytes = null;
            while(replicateResponseBytes == null){
                replicateResponseBytes = dataStub.writeBlock(replicateRequest.toByteArray());
            }

            this.requestsFulfilled.replace(replicateRequestId, true);
            ProtoHDFS.Response replicateResponse = ProtoHDFS.Response.parseFrom(replicateResponseBytes);
            ProtoHDFS.Response.ResponseType repStatus = replicateResponse.getResponseType();
            if(repStatus == ProtoHDFS.Response.ResponseType.FAILURE){
                ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
                responseBuilder.setResponseId(requestId);
                responseBuilder.setErrorMessage("Failed to replicate block");
                ProtoHDFS.Response response = responseBuilder.buildPartial();
                responseBuilder.clear();
                return response.toByteArray();
            }
        }

        String blockName = fileName + "_" + blockNumber + "_" + repNumber;
        File file = new File(blockName);
        FileOutputStream fileOutputStream;
        if(file.exists() || file.createNewFile()){
            fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(blockContents.getBytes());
            fileOutputStream.flush();
            fileOutputStream.close();
            blockMeta = blockMeta.toBuilder().setInitialized(true).build();
            this.blockMetas.putIfAbsent(blockName, blockMeta);
        }else{
            ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
            responseBuilder.setResponseId(requestId);
            responseBuilder.setErrorMessage("Failed to write block to data node");
            ProtoHDFS.Response response = responseBuilder.buildPartial();
            responseBuilder.clear();

            this.requestsFulfilled.replace(requestId, true);
            return response.toByteArray();
        }

        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
        responseBuilder.setErrorMessage(String.format("Block %1$d replication %2$d for %3$s write success",
                blockNumber, repNumber, fileName));
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();

        this.requestsFulfilled.replace(requestId, true);
        return response.toByteArray();
    }

    public DataNodeInterface getDNStub(String dataId, String dataIp, int port){
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(dataIp, port);
                return (DataNodeInterface)registry.lookup(dataId);
            }catch(Exception ignored){}
        }
    }

    public NameNodeInterface getNNStub(String nameId, String nameIp, int port){
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(nameId, port);
                return (NameNodeInterface)registry.lookup(nameIp);
            }catch(Exception ignored){}
        }
    }

    public static void main(String[] args){
        // Upon startup of the data node
        Properties prop = new Properties();
        try{
            // Bind data node to registry
            String dataNodeId = UUID.randomUUID().toString();
            String dataNodeIp = InetAddress.getLocalHost().getHostAddress();
            int dataPort = (args.length == 0) ? 1099 : Integer.parseInt(args[0]);

            Registry serverRegistry = LocateRegistry.createRegistry(dataPort);
            DataNode newDataNode = (args.length == 0) ? new DataNode(dataNodeId, dataNodeIp) :
                    new DataNode(dataNodeId, dataNodeIp, dataPort);
            serverRegistry.bind(dataNodeId, newDataNode);

            System.out.println("Data Node " + dataNodeId + " is running on host " + dataNodeIp + " port " + dataPort);

            // Gets the file handle to the namenode.properties file
            File propFile = new File("namenode.properties");
            FileInputStream fileInputStream = new FileInputStream(propFile);
            prop.load(fileInputStream);

            String nameNodeId = prop.getProperty("server_name");
            String nameNodeIp = prop.getProperty("server_ip");
            int namePort = Integer.parseInt(prop.getProperty("server_port"));

            // Connect data node to the name node and start sending heartbeats and block reports
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
            scheduledExecutorService.scheduleAtFixedRate(
                    new SendHeartbeatBlockReportTask(nameNodeId, nameNodeIp, namePort, newDataNode),
                    0, 5, TimeUnit.SECONDS);
        }catch(Exception e){
            System.out.println("An error has occurred: " + e.getMessage());
        }
    }
}
