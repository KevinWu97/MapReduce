package ds.hdfs;

import com.google.protobuf.InvalidProtocolBufferException;
import proto.ProtoHDFS;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class NameNode implements NameNodeInterface {
    protected Registry serverRegistry;
    protected ConcurrentHashMap<String, Boolean> requestsFulfilled;
    protected ConcurrentHashMap<String, ProtoHDFS.FileHandle> fileHandles;
    protected ConcurrentHashMap<String, ReentrantReadWriteLock> fileLocks;
    protected String nameId;
    protected String nameIp;
    protected int port;

    @Override
    public byte[] openFile(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();
        ProtoHDFS.Request.RequestType operation = request.getRequestType();
        ProtoHDFS.FileHandle fileHandle = request.getFileHandle();

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
                return response.toByteArray();
            }
        }else{
            // If file does not exist, assign the blocks of the file to different data nodes
            return assignBlock(inp);
        }

        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.FAILURE);
        responseBuilder.setErrorMessage("Invalid Request Type!");
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();
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
        for(ProtoHDFS.Pipeline p : pipelines){
            p.getBlocksList().sort(new RepSorter());
        }
        pipelines.sort(new PipelineSorter());

        ProtoHDFS.Response.Builder responseBuilder = ProtoHDFS.Response.newBuilder();
        responseBuilder.setResponseId(requestId);
        responseBuilder.setResponseType(ProtoHDFS.Response.ResponseType.SUCCESS);
        responseBuilder.setFileHandle(responseFileHandle);
        responseBuilder.setErrorMessage("File handle for " + fileName + " successfully obtained");
        ProtoHDFS.Response response = responseBuilder.buildPartial();
        responseBuilder.clear();

        return response.toByteArray();
    }

    @Override
    public byte[] assignBlock(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        // To assign blocks, we first get the number of blocks that will be needed
        // For each block we create a "pipeline" of Data Nodes where the blocks are written to and replicated
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

        // At this stage since the file hasn't been stored in the HDFS yet, the fileHandle really only contains the
        // filename as well as the file size which will be used to compute number of blocks needed. The pipeline
        // variable in fileHandle at this point should be empty
        ProtoHDFS.FileHandle fileHandle = request.getFileHandle();
        String fileName = fileHandle.getFileName();
        int fileSize = fileHandle.getFileSize();

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
                return response.toByteArray();
            }
        }

        ReentrantReadWriteLock lock = this.fileLocks.get(fileName);
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();

        // Make block size and replication factor to be configurable later
        int blockSize = 64000000;
        int repFactor = 3;
        int numBlocks = fileSize / blockSize + 1;

        ProtoHDFS.FileHandle.Builder fileHandleBuilder = ProtoHDFS.FileHandle.newBuilder();
        ArrayList<ProtoHDFS.Pipeline> pipelines = new ArrayList<>();
        for(int i = 0; i < numBlocks; i++){
            ProtoHDFS.Pipeline.Builder pipelineBuilder = ProtoHDFS.Pipeline.newBuilder();
            ArrayList<ProtoHDFS.Block> blocks = new ArrayList<>();

            // This part picks three random data nodes using the Data Node Ids
            String[] dataNodes = this.serverRegistry.list();
            List<String> dataNodesList = Arrays.asList(dataNodes);
            Collections.shuffle(dataNodesList);
            List<String> selectedDataNodes = dataNodesList.subList(0, repFactor);

            for(int j = 0; j < repFactor; j++){
                ProtoHDFS.BlockMeta.Builder blockMetaBuilder = ProtoHDFS.BlockMeta.newBuilder();
                blockMetaBuilder.setFileName(fileName);
                blockMetaBuilder.setBlockNumber(i);
                blockMetaBuilder.setRepNumber(j);
                blockMetaBuilder.setDataId(selectedDataNodes.get(j));
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

        return response.toByteArray();
    }

    @Override
    public byte[] list(byte[] inp) throws RemoteException, InvalidProtocolBufferException {
        ProtoHDFS.Request request = ProtoHDFS.Request.parseFrom(inp);
        String requestId = request.getRequestId();

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

        return listResponse.toByteArray();
    }

    @Override
    public byte[] blockReport(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    @Override
    public byte[] heartBeat(byte[] inp) throws RemoteException {
        return new byte[0];
    }

    public static void main(String[] args){

    }
}
