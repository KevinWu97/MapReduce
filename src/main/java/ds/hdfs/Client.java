package ds.hdfs;

import com.google.protobuf.InvalidProtocolBufferException;
import proto.ProtoHDFS;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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

    // This method stores the file in the HDFS
    public void putFile(String fileName) throws IOException {
        System.out.println("Going to put file " + fileName);
        File file = new File(fileName);

        // Make block size configurable later
        int blockSize = 64000000;
        int numBlocks = (int) (file.length() / blockSize + 1);
        ArrayList<byte[]> blocks = new ArrayList<>();

        try {
            byte[] blockContents = new byte[blockSize];
            FileInputStream fileInputStream = new FileInputStream(file);
            while(fileInputStream.read(blockContents) != -1){
                blocks.add(blockContents);
            }
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Just a sanity check
        assert blocks.size() == numBlocks : "Something went wrong! Did not properly read file into blocks!";

        ProtoHDFS.FileHandle.Builder fileHandleBuilder = ProtoHDFS.FileHandle.newBuilder();
        fileHandleBuilder.setFileName(fileName);
        fileHandleBuilder.setFileSize(file.length());
        ProtoHDFS.FileHandle fileHandle = fileHandleBuilder.buildPartial();

        ProtoHDFS.Request.Builder requestBuilder = ProtoHDFS.Request.newBuilder();
        String requestId = UUID.randomUUID().toString();
        requestBuilder.setRequestId(requestId);
        requestBuilder.setRequestType(ProtoHDFS.Request.RequestType.WRITE);
        requestBuilder.setFileHandle(fileHandle);
        ProtoHDFS.Request openRequest = requestBuilder.buildPartial();
        requestBuilder.clear();

        // Read these variables from the config file later
        String nameId = "namenode";
        String nameIp = "192.168.12.75";
        int port = 1099;

        NameNodeInterface nameStub = getNameStub(nameId, nameIp, port);
        byte[] openResponseBytes = nameStub.openFile(openRequest.toByteArray());

        ProtoHDFS.Response openResponse = ProtoHDFS.Response.parseFrom(openResponseBytes);
        String responseId = openResponse.getResponseId();
        ProtoHDFS.Response.ResponseType openResponseType = openResponse.getResponseType();
        if(openResponseType == ProtoHDFS.Response.ResponseType.SUCCESS){
            // If write file completed successfully send write requests to the data nodes
            // using the file handle obtained from the response
            System.out.println("File " + fileName + " successfully opened");

            fileHandle = openResponse.getFileHandle();
            List<ProtoHDFS.Pipeline> pipelineList = fileHandle.getPipelinesList();
            ArrayList<ProtoHDFS.Pipeline> pipelineArrayList = new ArrayList<>(pipelineList);
            for(int i = 0; i < numBlocks; i++){
                byte[] blockContent = blocks.get(i);
                ProtoHDFS.Pipeline pipeline = pipelineArrayList.get(i);
                List<ProtoHDFS.Block> blocksList = pipeline.getBlocksList();

                ArrayList<ProtoHDFS.Block> requestBlocks = new ArrayList<>();
                for(ProtoHDFS.Block block : blocksList){
                    ProtoHDFS.Block.Builder blockBuilder = ProtoHDFS.Block.newBuilder();
                    blockBuilder.setBlockMeta(block.getBlockMeta());
                    blockBuilder.setBlockContents(Arrays.toString(blockContent));
                    ProtoHDFS.Block requestBlock = blockBuilder.build();
                    blockBuilder.clear();
                    requestBlocks.add(requestBlock);
                }

                String writeRequestId = UUID.randomUUID().toString();
                requestBuilder.setRequestId(writeRequestId);
                requestBuilder.setRequestType(ProtoHDFS.Request.RequestType.WRITE);
                requestBuilder.addAllBlock(requestBlocks);
                ProtoHDFS.Request writeBlockRequest = requestBuilder.buildPartial();
                requestBuilder.clear();

                // Configure these variables later
                String dataId = "data1";
                String dataIp = "192.168.12.1";
                int dataPort = 1099;

                DataNodeInterface dataStub = getDataStub(dataId, dataIp, dataPort);
                byte[] writeResponseBytes = dataStub.writeBlock(writeBlockRequest.toByteArray());
                ProtoHDFS.Response writeResponse = ProtoHDFS.Response.parseFrom(writeResponseBytes);
                String writeResponseId = writeResponse.getResponseId();
                ProtoHDFS.Response.ResponseType writeResponseType = writeResponse.getResponseType();

                if(writeResponseType == ProtoHDFS.Response.ResponseType.SUCCESS){
                    System.out.println("File " + fileName + " successfully written");
                }else{
                    System.out.println(writeResponse.getErrorMessage());
                    return;
                }
            }
        }else{
            // If failed to open and get file handle
            System.out.println(openResponse.getErrorMessage());
            return;
        }

        // Now send a close request to close (or unlock) the other file handle so other threads can use it
        String closeRequestId = UUID.randomUUID().toString();
        requestBuilder.setRequestId(closeRequestId);
        requestBuilder.setRequestType(ProtoHDFS.Request.RequestType.CLOSE);
        ProtoHDFS.Request closeRequest = requestBuilder.buildPartial();
        requestBuilder.clear();

        byte[] closeResponseBytes = nameStub.closeFile(closeRequest.toByteArray());
        ProtoHDFS.Response closeResponse = ProtoHDFS.Response.parseFrom(closeResponseBytes);
        String closeResponseId = closeResponse.getResponseId();
        ProtoHDFS.Response.ResponseType closeResponseType = closeResponse.getResponseType();

        // !!!!!!!!!!! We need to implement something that allows it to keep sending close requests until
        // file handle fails to unlock. Otherwise we'll run into deadlock !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        if(closeResponseType == ProtoHDFS.Response.ResponseType.SUCCESS){
            System.out.println("File handle for " + fileName + " successfully closed");
        }else{
            System.out.println(closeResponse.getErrorMessage());
        }
    }

    public void getFile(String fileName) {
        System.out.println("Going to get " + fileName);
        File file = new File(fileName);
        FileOutputStream fileOutputStream;
        ProtoHDFS.Request.Builder requestBuilder = ProtoHDFS.Request.newBuilder();

        try{
            if(file.exists() || file.createNewFile()){
                fileOutputStream = new FileOutputStream(file);

                ProtoHDFS.FileHandle.Builder fileHandleBuilder = ProtoHDFS.FileHandle.newBuilder();
                fileHandleBuilder.setFileName(fileName);
                fileHandleBuilder.setFileSize(file.length());
                ProtoHDFS.FileHandle fileHandle = fileHandleBuilder.buildPartial();
                fileHandleBuilder.clear();

                String openRequestId = UUID.randomUUID().toString();
                requestBuilder.setRequestId(openRequestId);
                requestBuilder.setRequestType(ProtoHDFS.Request.RequestType.READ);
                requestBuilder.setFileHandle(fileHandle);
                ProtoHDFS.Request openRequest = requestBuilder.buildPartial();
                requestBuilder.clear();

                // Configure these values later
                String nameId = "namenode";
                String nameIp = "192.168.12.75";
                int port = 1099;

                NameNodeInterface nameStub = getNameStub(nameId, nameIp, port);
                byte[] openResponseBytes = nameStub.openFile(openRequest.toByteArray());

                ProtoHDFS.Response openResponse = ProtoHDFS.Response.parseFrom(openResponseBytes);
                String openResponseId = openResponse.getResponseId();
                ProtoHDFS.Response.ResponseType openResponseType = openResponse.getResponseType();
                fileHandle = openResponse.getFileHandle();

                List<ProtoHDFS.Pipeline> pipelines = fileHandle.getPipelinesList();
                ArrayList<List<ProtoHDFS.Block>> blocksList = pipelines.stream()
                        .map(ProtoHDFS.Pipeline::getBlocksList)
                        .collect(Collectors.toCollection(ArrayList::new));
                boolean hasMissingBlock = blocksList.parallelStream().anyMatch(List::isEmpty);

                if(hasMissingBlock){
                    // If the list of block replicas is empty for any of the blocks, immediately throw an error
                    // Maybe toss out the file as well since it's corrupted?
                }else{
                    ArrayList<ProtoHDFS.Block> readBlocks = blocksList.stream()
                            .map(p -> p.get(0))
                            .collect(Collectors.toCollection(ArrayList::new));
                    // Writes the contents from a copy of each block into the fileOutputStream
                    for (ProtoHDFS.Block b : readBlocks) {
                        byte[] bytes = b.getBlockContents().getBytes();
                        fileOutputStream.write(bytes);
                    }
                }
            }else{
                System.out.println("Failed to create " + fileName + " to read to");
            }
        }catch(Exception e){
            System.out.println("File " + fileName + " not found!");
        }
    }

    public void list() throws InvalidProtocolBufferException, RemoteException {
        ProtoHDFS.Request.Builder listRequestBuilder = ProtoHDFS.Request.newBuilder();
        String listRequestId = UUID.randomUUID().toString();
        listRequestBuilder.setRequestId(listRequestId);
        listRequestBuilder.setRequestType(ProtoHDFS.Request.RequestType.LIST);
        ProtoHDFS.Request listRequest = listRequestBuilder.buildPartial();
        listRequestBuilder.clear();

        // Read these variables from the config file later
        String nameId = "namenode";
        String nameIp = "192.168.12.75";
        int port = 1099;

        NameNodeInterface nameStub = getNameStub(nameId, nameIp, port);
        byte[] listResponseBytes = nameStub.list(listRequest.toByteArray());
        ProtoHDFS.ListResponse listResponse = ProtoHDFS.ListResponse.parseFrom(listResponseBytes);
        String listResponseId = listResponse.getResponseId();
        ProtoHDFS.ListResponse.ResponseType listResponseType = listResponse.getResponseType();

        if(listResponseType == ProtoHDFS.ListResponse.ResponseType.SUCCESS){
            // Gets the list of files and prints it out line by line
            List<String> filesList = listResponse.getFileNamesList();
            //noinspection SimplifyStreamApiCallChains
            filesList.stream().forEach(System.out :: println);
        }else{
            System.out.println(listResponse.getErrorMessage());
        }
    }

    public static void main(String[] args){

    }
}
