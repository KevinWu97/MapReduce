package ds.hdfs;

import com.google.protobuf.InvalidProtocolBufferException;
import proto.ProtoHDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class NameNode extends UnicastRemoteObject implements NameNodeInterface {
    protected Registry serverRegistry;
    protected ConcurrentHashMap<String, Boolean> requestsFulfilled;
    protected ConcurrentHashMap<String, ProtoHDFS.NodeMeta> dataNodeMetas;
    protected ConcurrentHashMap<String, Instant> heartbeatTimestamps;
    protected ConcurrentHashMap<String, List<ProtoHDFS.BlockMeta>> dataNodeBlockMetas;
    protected ConcurrentHashMap<String, ProtoHDFS.FileHandle> fileHandles;
    protected ConcurrentHashMap<String, ReentrantReadWriteLock> fileLocks;
    protected String nameId;
    protected String nameIp;
    protected int port;

    protected NameNode() throws RemoteException {
        super();
    }

    protected NameNode(int port) throws RemoteException {
        super(port);
    }

    @Override
    public byte[] openFile(byte[] inp) throws IOException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();
        ProtoHDFS.Request.RequestType operation = request.getRequestType();
        ProtoHDFS.FileHandle fileHandle = request.getFileHandle();
        if(!this.requestsFulfilled.containsKey(requestId) || !this.requestsFulfilled.get(requestId)){
            this.requestsFulfilled.putIfAbsent(requestId, false);
        }

        String fileName = fileHandle.getFileName();
        if(this.fileHandles.containsKey(fileName)){
            // If file does exist, first check if it is a read or write request
            // If read request, return a file handle
            // If write request, return an error response
            if(operation == ProtoHDFS.Request.RequestType.READ){
                return getBlockLocations(inp);
            }else if(operation == ProtoHDFS.Request.RequestType.WRITE){
                ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
                responseBuilder.setResponseId(requestId);
                responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.FAILURE);
                responseBuilder.setErrorMessage("File " + fileName + " already exists! Write failed!");
                ProtoHDFS.Response response = responseBuilder.buildPartial();
                responseBuilder.clear();
                this.requestsFulfilled.replace(requestId, true);
                return response.toByteArray();
            }
        }else{
            // If file does not exist and its a write request assign the blocks of the file to different data nodes
            // If its a read request, return an error
            if(operation == ProtoHDFS.Request.RequestType.WRITE){
                return assignBlock(inp);
            }else if(operation == ProtoHDFS.Request.RequestType.READ){
                ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
                responseBuilder.setResponseId(requestId);
                responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.FAILURE);
                responseBuilder.setErrorMessage("File " + fileName + " does not exists! Read failed!");
                ProtoHDFS.Response response = responseBuilder.buildPartial();
                responseBuilder.clear();
                this.requestsFulfilled.replace(requestId, true);
                return response.toByteArray();
            }
        }

        // Should not happen
        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.FAILURE);
        responseBuilder.setErrorMessage("Invalid Request Type!");
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();
        this.requestsFulfilled.replace(requestId, true);
        return response.toByteArray();
    }

    @Override
    public byte[] closeFile(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

        ProtoHDFS.FileHandle requestFileHandle = request.getFileHandle();
        String fileName = requestFileHandle.getFileName();

        ReentrantReadWriteLock lock = this.fileLocks.get(fileName);
        if(lock.isWriteLockedByCurrentThread()){
            ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
            writeLock.unlock();
        }else{
            ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
            readLock.unlock();
        }

        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
        responseBuilder.setErrorMessage("File handle for " + fileName + " successfully closed");
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();
        return response.toByteArray();
    }

    @Override
    public byte[] getBlockLocations(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        // To assign blocks, we first get the number of blocks that will be needed
        // For each block we create a "pipeline" of Data Nodes where the blocks are written to and replicated
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

        ProtoHDFS.FileHandle requestFileHandle = request.getFileHandle();
        String fileName = requestFileHandle.getFileName();

        ReentrantReadWriteLock lock = this.fileLocks.get(fileName);
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();

        ProtoHDFS.FileHandle responseFileHandle = this.fileHandles.get(fileName);
        List<ProtoHDFS.Pipeline> pipelines = responseFileHandle.getPipelinesList();
        List<ProtoHDFS.Pipeline> newPipelines = new ArrayList<>();

        ProtoHDFS.FileHandle.Builder fileHandleBuilder = ProtoHDFS.FileHandle.newBuilder();
        ProtoHDFS.Pipeline.Builder pipeLineBuilder = ProtoHDFS.Pipeline.newBuilder();

        for(ProtoHDFS.Pipeline p : pipelines){
            p.getBlocksList().sort(new RepSorter());
            ArrayList<ProtoHDFS.Block> newBlocksList = p.getBlocksList().stream()
                    .filter(ProtoHDFS.Block::isInitialized)
                    .collect(Collectors.toCollection(ArrayList::new));

            pipeLineBuilder.addAllBlocks(newBlocksList);
            ProtoHDFS.Pipeline newPipeline = pipeLineBuilder.build();
            pipeLineBuilder.clear();
            newPipelines.add(newPipeline);
        }

        newPipelines.sort(new PipelineSorter());
        fileHandleBuilder.setFileName(fileName);
        fileHandleBuilder.setFileSize(responseFileHandle.getFileSize());
        fileHandleBuilder.addAllPipelines(newPipelines);
        responseFileHandle = fileHandleBuilder.build();
        fileHandleBuilder.clear();

        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
        responseBuilder.setFileHandle(responseFileHandle);
        responseBuilder.setErrorMessage("File handle for " + fileName + " successfully obtained");
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();

        this.requestsFulfilled.replace(requestId, true);
        return response.toByteArray();
    }

    @Override
    public byte[] assignBlock(byte[] inp) throws IOException {
        // To assign blocks, we first get the number of blocks that will be needed
        // For each block we create a "pipeline" of Data Nodes where the blocks are written to and replicated
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

        // At this stage since the file hasn't been stored in the HDFS yet, the fileHandle really only contains the
        // filename as well as the file size which will be used to compute number of blocks needed. The pipeline
        // variable in fileHandle at this point should be empty
        ProtoHDFS.FileHandle fileHandle = request.getFileHandle();
        String fileName = fileHandle.getFileName();
        long fileSize = fileHandle.getFileSize();

        synchronized (this){
            // This part locks the file handle once it has been created
            if(!this.fileLocks.containsKey(fileName)){
                // File has not yet been initialized by another thread so create file handle
                this.fileLocks.putIfAbsent(fileName, new ReentrantReadWriteLock());
            }else{
                // Another thread has already initialized the file so return error
                ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
                responseBuilder.setResponseId(requestId);
                responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.FAILURE);
                responseBuilder.setErrorMessage("File " + fileName + " has been created by another thread");
                ProtoHDFS.Response response = responseBuilder.buildPartial();
                responseBuilder.clear();
                this.requestsFulfilled.replace(requestId, true);
                return response.toByteArray();
            }
        }

        ReentrantReadWriteLock lock = this.fileLocks.get(fileName);
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();

        // Make block size and replication factor to be configurable later
        Properties hdfsProp = new Properties();
        File hdfsPropFile = new File("hdfs.properties");
        FileInputStream hdfsPropInputStream = new FileInputStream(hdfsPropFile);
        hdfsProp.load(hdfsPropInputStream);

        int blockSize = Integer.parseInt(hdfsProp.getProperty("block_size", "64000000"));
        int repFactor = Integer.parseInt(hdfsProp.getProperty("rep_factor", "3"));
        int numBlocks = (int) (fileSize / blockSize + 1);

        ProtoHDFS.FileHandle.Builder fileHandleBuilder = ProtoHDFS.FileHandle.newBuilder();
        ArrayList<ProtoHDFS.Pipeline> pipelines = new ArrayList<>();
        for(int i = 0; i < numBlocks; i++){
            ProtoHDFS.Pipeline.Builder pipelineBuilder = ProtoHDFS.Pipeline.newBuilder();
            ArrayList<ProtoHDFS.Block> blocks = new ArrayList<>();

            List<String> dataNodeKeys = Collections.list(this.dataNodeMetas.keys());
            ArrayList<ProtoHDFS.NodeMeta> dataNodesList = new ArrayList<>();
            for(String key : dataNodeKeys){
                dataNodesList.add(this.dataNodeMetas.get(key));
            }

            ArrayList<ProtoHDFS.NodeMeta> activeDataNodes = dataNodesList.stream()
                    .filter(nodeMeta -> Duration.between(heartbeatTimestamps.get(nodeMeta.getId()),
                            Instant.now()).toMillis() < 3000)
                    .collect(Collectors.toCollection(ArrayList::new));
            Collections.shuffle(dataNodesList);
            List<ProtoHDFS.NodeMeta> selectedDataNodes = activeDataNodes.subList(0, repFactor);

            for(int j = 0; j < repFactor; j++){
                ProtoHDFS.BlockMeta.Builder blockMetaBuilder = ProtoHDFS.BlockMeta.newBuilder();
                blockMetaBuilder.setFileName(fileName);
                blockMetaBuilder.setBlockNumber(i);
                blockMetaBuilder.setRepNumber(j);
                blockMetaBuilder.setDataId(selectedDataNodes.get(j).getId());
                blockMetaBuilder.setDataIp(selectedDataNodes.get(j).getIp());
                blockMetaBuilder.setPort(selectedDataNodes.get(j).getPort());
                blockMetaBuilder.setInitialized(false);
                ProtoHDFS.BlockMeta blockMeta = blockMetaBuilder.build();
                blockMetaBuilder.clear();

                ProtoHDFS.Block.Builder blockBuilder = ProtoHDFS.Block.newBuilder();
                blockBuilder.setBlockMeta(blockMeta);
                ProtoHDFS.Block block = blockBuilder.buildPartial();
                blockBuilder.clear();

                blocks.add(block);
            }

            pipelineBuilder.setPipelineNumber(i);
            pipelineBuilder.addAllBlocks(blocks);
            ProtoHDFS.Pipeline pipeline = pipelineBuilder.build();
            pipelineBuilder.clear();
            pipelines.add(pipeline);
        }

        fileHandleBuilder.setFileName(fileName);
        fileHandleBuilder.setFileSize(fileSize);
        fileHandleBuilder.addAllPipelines(pipelines);
        ProtoHDFS.FileHandle newFileHandle = fileHandleBuilder.build();
        fileHandleBuilder.clear();

        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
        responseBuilder.setFileHandle(newFileHandle);
        responseBuilder.setErrorMessage("File handle for " + fileName + " created successfully");
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();
        this.requestsFulfilled.replace(requestId, true);
        return response.toByteArray();
    }

    @Override
    public byte[] list(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();
        if(!this.requestsFulfilled.containsKey(requestId) || !this.requestsFulfilled.get(requestId)){
            this.requestsFulfilled.putIfAbsent(requestId, false);
        }

        Enumeration<String> fileKeys = this.fileHandles.keys();
        List<String> fileKeysList = Collections.list(fileKeys);
        List<String> sortedFileKeys = fileKeysList.stream().sorted().collect(Collectors.toList());

        ProtoHDFS.ListResponse.Builder listResponseBuilder = ProtoHDFS.ListResponse.newBuilder();
        listResponseBuilder.setResponseId(requestId);
        listResponseBuilder.setResponseType(ProtoHDFS.ListResponse.ResponseType.SUCCESS);
        listResponseBuilder.setErrorMessage("Files on HDFS successfully retrieved");
        listResponseBuilder.addAllFileNames(sortedFileKeys);
        ProtoHDFS.ListResponse listResponse = listResponseBuilder.build();
        listResponseBuilder.clear();

        this.requestsFulfilled.replace(requestId, true);
        return listResponse.toByteArray();
    }

    @Override
    public synchronized byte[] blockReport(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        ProtoHDFS.BlockReport blockReport = ProtoHDFS.BlockReport.parseFrom(inp);
        String dataId = blockReport.getDataId();
        List<ProtoHDFS.BlockMeta> dataBlockMetas = blockReport.getDataNodeBlocksList();

        ProtoHDFS.BlockMeta.Builder blockMetaBuilder = ProtoHDFS.BlockMeta.newBuilder();
        ProtoHDFS.Block.Builder blockBuilder = ProtoHDFS.Block.newBuilder();

        for(ProtoHDFS.BlockMeta b : dataBlockMetas){
            String fileName = b.getFileName();
            int blockNumber = b.getBlockNumber();
            int repNumber = b.getRepNumber();
            String dataIp = b.getDataIp();
            int dataPort = b.getPort();

            if(!this.fileHandles.get(fileName).getPipelines(blockNumber).getBlocks(repNumber)
                    .getBlockMeta().getInitialized()){
                blockMetaBuilder.setFileName(fileName);
                blockMetaBuilder.setBlockNumber(blockNumber);
                blockMetaBuilder.setRepNumber(repNumber);
                blockMetaBuilder.setDataId(dataId);
                blockMetaBuilder.setDataIp(dataIp);
                blockMetaBuilder.setPort(dataPort);
                blockMetaBuilder.setInitialized(true);
                ProtoHDFS.BlockMeta newBlockMeta = blockMetaBuilder.build();
                blockMetaBuilder.clear();

                blockBuilder.setBlockMeta(newBlockMeta);
                ProtoHDFS.Block newBlock = blockBuilder.buildPartial();
                blockBuilder.clear();

                this.fileHandles.get(fileName).getPipelines(blockNumber).getBlocksList().set(repNumber, newBlock);
            }
        }

        this.dataNodeBlockMetas.replace(dataId, dataBlockMetas);

        ProtoHDFS.NodeMeta.Builder nodeMetaBuilder = ProtoHDFS.NodeMeta.newBuilder();
        nodeMetaBuilder.setId(this.nameId);
        nodeMetaBuilder.setIp(this.nameIp);
        nodeMetaBuilder.setPort(this.port);
        ProtoHDFS.NodeMeta nodeMeta = nodeMetaBuilder.build();
        nodeMetaBuilder.clear();

        return nodeMeta.toByteArray();
    }

    @Override
    public synchronized byte[] heartBeat(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        ProtoHDFS.Heartbeat heartbeat = ProtoHDFS.Heartbeat.parseFrom(inp);
        ProtoHDFS.NodeMeta dataNodeMeta = heartbeat.getDataNodeMeta();
        String dataNodeId = dataNodeMeta.getId();

        if(this.heartbeatTimestamps.containsKey(dataNodeId)){
            this.heartbeatTimestamps.replace(dataNodeId, Instant.now());
        }else{
            this.heartbeatTimestamps.put(dataNodeId, Instant.now());
            this.dataNodeMetas.put(dataNodeId, dataNodeMeta);
        }

        ProtoHDFS.NodeMeta.Builder nodeMetaBuilder = ProtoHDFS.NodeMeta.newBuilder();
        nodeMetaBuilder.setId(this.nameId);
        nodeMetaBuilder.setIp(this.nameIp);
        nodeMetaBuilder.setPort(this.port);
        ProtoHDFS.NodeMeta nodeMeta = nodeMetaBuilder.build();
        nodeMetaBuilder.clear();

        return nodeMeta.toByteArray();
    }

    public static void main(String[] args){
        // Starts up the name node server and configures properties of the namenode
        Properties props = new Properties();
        File file = new File("namenode.properties");
        try{
            String nodeName = UUID.randomUUID().toString();
            InetAddress inetAddress = InetAddress.getLocalHost();
            String nodeIp = inetAddress.getHostAddress();
            int nodePort = (args.length == 0) ? 1099 : Integer.parseInt(args[0]);

            FileOutputStream fileOutputStream = new FileOutputStream(file);
            props.setProperty("server_name", nodeName);
            props.setProperty("server_ip", nodeIp);
            props.setProperty("server_port", String.valueOf(nodePort));
            props.store(fileOutputStream, null);

            Registry serverRegistry = LocateRegistry.createRegistry(nodePort);
            serverRegistry.bind(nodeName, (args.length == 0) ? new NameNode() : new NameNode(nodePort));

            System.out.println("Name Node " + nodeName + " is running on host " + nodeIp + " port " + nodePort);

        }catch(Exception e){
            System.out.println("Error occurred when starting the Name Node: " + e.getMessage());
        }
    }
}
