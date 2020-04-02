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

    public Client(){
        
    }

    public static DataNodeInterface getDataStub(String dataId, String dataIp, int port){
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(dataIp, port);
                return (DataNodeInterface)registry.lookup(dataId);
            }catch(Exception ignored){}
        }
    }

    public static NameNodeInterface getNameStub(String nameId, String nameIp, int port){
        while(true){
            try{
                Registry registry = LocateRegistry.getRegistry(nameId, port);
                return (NameNodeInterface)registry.lookup(nameIp);
            }catch(Exception ignored){}
        }
    }

    // This method stores the file in the HDFS
    public static void putFile(String localFile, String hdfsFile) {
        System.out.println("Going to put file " + localFile + " into HDFS as " + hdfsFile);
        File file = new File(localFile);

        try{
            // Make block size configurable later
            int blockSize = 64000000;
            int numBlocks = (int) (file.length() / blockSize + 1);
            ArrayList<byte[]> blocks = new ArrayList<>();

            byte[] blockContents = new byte[blockSize];
            FileInputStream fileInputStream = new FileInputStream(file);
            while(fileInputStream.read(blockContents) != -1){
                blocks.add(blockContents);
            }
            fileInputStream.close();

            // Just a sanity check
            assert blocks.size() == numBlocks : "Something went wrong! Did not properly read file into blocks!";

            ProtoHDFS.FileHandle.Builder fileHandleBuilder = ProtoHDFS.FileHandle.newBuilder();
            fileHandleBuilder.setFileName(hdfsFile);
            fileHandleBuilder.setFileSize(file.length());
            ProtoHDFS.FileHandle fileHandle = fileHandleBuilder.buildPartial();
            fileHandleBuilder.clear();

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
            // OPEN file handle using name node
            byte[] openResponseBytes = null;
            while(openResponseBytes == null){
                openResponseBytes = nameStub.openFile(openRequest.toByteArray());
            }

            ProtoHDFS.Response openResponse = ProtoHDFS.Response.parseFrom(openResponseBytes);
            String responseId = openResponse.getResponseId();
            ProtoHDFS.Response.ResponseType openResponseType = openResponse.getResponseType();
            if(openResponseType == ProtoHDFS.Response.ResponseType.SUCCESS){
                // If write file completed successfully send write requests to the data nodes
                // using the file handle obtained from the response
                System.out.println("File handle for " + localFile + " successfully opened as " + hdfsFile);

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

                    // WRITE to the data nodes
                    DataNodeInterface dataStub = getDataStub(dataId, dataIp, dataPort);
                    byte[] writeResponseBytes = dataStub.writeBlock(writeBlockRequest.toByteArray());

                    ProtoHDFS.Response writeResponse = ProtoHDFS.Response.parseFrom(writeResponseBytes);
                    String writeResponseId = writeResponse.getResponseId();
                    ProtoHDFS.Response.ResponseType writeResponseType = writeResponse.getResponseType();

                    if(writeResponseType == ProtoHDFS.Response.ResponseType.SUCCESS){
                        System.out.println("File " + localFile + " successfully stored in HDFS as " + hdfsFile);
                    }else{
                        System.out.println("Storing file " + localFile + " as " + hdfsFile + " failed");
                    }
                }
            }else{
                // If failed to open and get file handle
                System.out.println("File handle for " + localFile + " failed to open as " + hdfsFile);
            }

            // Now send a close request to close (or unlock) the other file handle so other threads can use it
            String closeRequestId = UUID.randomUUID().toString();
            requestBuilder.setRequestId(closeRequestId);
            requestBuilder.setRequestType(ProtoHDFS.Request.RequestType.CLOSE);
            ProtoHDFS.Request closeRequest = requestBuilder.buildPartial();
            requestBuilder.clear();

            // CLOSE file handle here
            byte[] closeResponseBytes = null;
            while(closeResponseBytes == null){
                closeResponseBytes = nameStub.closeFile(closeRequest.toByteArray());
            }
            ProtoHDFS.Response closeResponse = ProtoHDFS.Response.parseFrom(closeResponseBytes);

            String closeResponseId = closeResponse.getResponseId();
            ProtoHDFS.Response.ResponseType closeResponseType = closeResponse.getResponseType();

            // !!!!!!!!!!! We need to implement something that allows it to keep sending close requests until
            // file handle unlocks. Otherwise we'll run into deadlock !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            if(closeResponseType == ProtoHDFS.Response.ResponseType.SUCCESS){
                System.out.println("File handle for " + localFile + " successfully closed on HDFS as " + hdfsFile);
            }else{
                System.out.println("File handle for " + localFile + " failed to close on HDFS as " + hdfsFile);
            }
        }catch(Exception e){
            if(e instanceof RemoteException){
                System.out.println("Something went wrong when working with name node stub or data node stub!");
            }else if(e instanceof InvalidProtocolBufferException){
                System.out.println("Tried to parse object in put() that is not defined in protocol buffer!");
            }else if(e instanceof FileNotFoundException){
                System.out.println("Trying to put() " + localFile + " that does not exist locally!");
            }else if(e instanceof IOException){
                System.out.println("Something went wrong when performing file io in put()!");
            }else{
                // Some general unspecified error
                System.out.println("An unspecified error has occurred in put(): " + e.getMessage());
            }
            e.printStackTrace();
        }
    }

    public static void getFile(String localFile, String hdfsFile) {
        System.out.println("Going to get " + hdfsFile);
        // This file handle is for the local file you want to read to
        File file = new File(localFile);

        try{
            ProtoHDFS.Request.Builder requestBuilder = ProtoHDFS.Request.newBuilder();
            if(file.exists() || file.createNewFile()){
                FileOutputStream fileOutputStream = new FileOutputStream(file, true);

                ProtoHDFS.FileHandle.Builder fileHandleBuilder = ProtoHDFS.FileHandle.newBuilder();
                fileHandleBuilder.setFileName(hdfsFile);
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
                byte[] openResponseBytes = null;
                while(openResponseBytes == null){
                    openResponseBytes = nameStub.openFile(openRequest.toByteArray());
                }

                ProtoHDFS.Response openResponse = ProtoHDFS.Response.parseFrom(openResponseBytes);
                String openResponseId = openResponse.getResponseId();
                ProtoHDFS.Response.ResponseType openResponseType = openResponse.getResponseType();

                if(openResponseType == ProtoHDFS.Response.ResponseType.SUCCESS){
                    // If file successfully opened, read the file
                    fileHandle = openResponse.getFileHandle();
                    List<ProtoHDFS.Pipeline> pipelines = fileHandle.getPipelinesList();
                    ArrayList<List<ProtoHDFS.Block>> repsList = pipelines.stream()
                            .map(ProtoHDFS.Pipeline::getBlocksList)
                            .collect(Collectors.toCollection(ArrayList::new));
                    boolean hasMissingBlock = repsList.parallelStream().anyMatch(List::isEmpty);

                    // Need to fix here. Doesn't actually read the block
                    if(hasMissingBlock){
                        System.out.println("Error: File " + hdfsFile + " corrupted when reading to " + localFile);
                    }else{
                        // Get list of blocks. Then send read request to the data nodes containing each block
                        List<ProtoHDFS.Block> blockList = repsList.stream()
                                .map(blocks -> blocks.get(0))
                                .collect(Collectors.toCollection(ArrayList::new));
                        for(ProtoHDFS.Block b : blockList){
                            String readRequestId = UUID.randomUUID().toString();
                            requestBuilder.setRequestId(readRequestId);
                            requestBuilder.setRequestType(ProtoHDFS.Request.RequestType.READ);
                            requestBuilder.addBlock(b);
                            ProtoHDFS.Request readRequest = requestBuilder.buildPartial();
                            requestBuilder.clear();

                            String dataId = b.getBlockMeta().getDataId();
                            String dataIp = b.getBlockMeta().getDataIp();
                            int dataPort = b.getBlockMeta().getPort();

                            DataNodeInterface dataNodeInterface = getDataStub(dataId, dataIp, dataPort);
                            byte[] readResponseBytes = null;
                            while(readResponseBytes == null){
                                readResponseBytes = dataNodeInterface.readBlock(readRequest.toByteArray());
                            }

                            ProtoHDFS.Response readResponse = ProtoHDFS.Response.parseFrom(readResponseBytes);
                            String readResponseId = readResponse.getResponseId();
                            ProtoHDFS.Response.ResponseType readResponseType = readResponse.getResponseType();

                            if(readResponseType == ProtoHDFS.Response.ResponseType.SUCCESS){
                                String blockContents = readResponse.getBlock().getBlockContents();
                                fileOutputStream.write(blockContents.getBytes());
                            }else{
                                System.out.println("Error, failed to read file blocks!");
                                if(file.delete()){
                                    System.out.println("File to read to has been deleted!");
                                }else{
                                    System.out.println("File to read to also failed to delete!");
                                }
                            }
                        }
                    }
                }else{
                    // If file failed to open, print error message
                    System.out.println("Failed to open file handle to " + hdfsFile + " while reading to " + localFile);
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
                    System.out.println("File handle for " + hdfsFile + " successfully closed reading to " + localFile);
                }else{
                    System.out.println("File handle for " + hdfsFile + " failed to close reading to " + localFile);
                }
            }else{
                System.out.println("Failed to create " + localFile + " to read " + hdfsFile);
            }
        }catch(Exception e){
            System.out.println("File " + hdfsFile + " not found in HDFS while reading to " + localFile);
        }
    }

    public static void list() {
        try{
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
            byte[] listResponseBytes = null;
            while(listResponseBytes == null){
                listResponseBytes = nameStub.list(listRequest.toByteArray());
            }
            ProtoHDFS.ListResponse listResponse = ProtoHDFS.ListResponse.parseFrom(listResponseBytes);
            String listResponseId = listResponse.getResponseId();
            ProtoHDFS.ListResponse.ResponseType listResponseType = listResponse.getResponseType();

            if(listResponseType == ProtoHDFS.ListResponse.ResponseType.SUCCESS){
                // Gets the list of files and prints it out line by line
                List<String> filesList = listResponse.getFileNamesList();
                //noinspection SimplifyStreamApiCallChains
                filesList.stream().forEach(System.out :: println);
            }else{
                // Shouldn't really happen
                System.out.println("list operation failed");
            }
        }catch(Exception e){
            if(e instanceof RemoteException){
                System.out.println("Something went wrong in list() when communicating with the name node!");
            }else if(e instanceof InvalidProtocolBufferException){
                System.out.println("Tried to parse something in list() that is not defined in the protocol buffer!");
            }else{
                // general unspecified error
                System.out.println("An unspecified error has occurred in list(): " + e.getMessage());
            }
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        // Input arguments are: command, input file, output file
        try{
            switch (args[0]) {
                case "put":
                    putFile(args[0], args[1]);
                    break;
                case "get":
                    getFile(args[0], args[1]);
                    break;
                case "delete":
                    // Coming soon :)
                    break;
                case "list":
                    list();
                    break;
                default:
                    throw new IOException();
            }
        }catch (IOException e){
            System.out.println("You have inputted the wrong number of arguments or an invalid command");
        }
    }
}
