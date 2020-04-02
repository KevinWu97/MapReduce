package ds.hdfs;

import proto.ProtoHDFS;

import java.io.*;

public class Test {
    public static void main(String[] args){

        ProtoHDFS.BlockMeta.Builder blockMetaBuilder = ProtoHDFS.BlockMeta.newBuilder();
        blockMetaBuilder.setFileName("jojo");
        blockMetaBuilder.setBlockNumber(8);
        blockMetaBuilder.setRepNumber(2);
        blockMetaBuilder.setDataId("dataname");
        blockMetaBuilder.setDataIp("ipaddress");
        blockMetaBuilder.setPort(1099);
        blockMetaBuilder.setInitialized(false);
        ProtoHDFS.BlockMeta blockMeta = blockMetaBuilder.build();
        blockMetaBuilder.clear();

        System.out.print(blockMeta.getFileName() + " ");
        System.out.print(blockMeta.getBlockNumber() + " ");
        System.out.print(blockMeta.getInitialized() + " ");

        blockMeta = blockMeta.toBuilder().setInitialized(true).build();

        System.out.println("");
        System.out.print(blockMeta.getFileName() + " ");
        System.out.print(blockMeta.getBlockNumber() + " ");
        System.out.print(blockMeta.getInitialized() + " ");
        /*
        File file = new File("randomFile.txt");
        File out = new File("outFile.txt");
        int readBytes = 0;
        try {
            byte[] b = new byte[10];
            if((file.exists() || file.createNewFile() && (out.exists() || out.createNewFile()))){
                FileInputStream fileInputStream = new FileInputStream(file);
                FileOutputStream fileOutputStream = new FileOutputStream(out);
                while((readBytes = fileInputStream.read(b)) != -1){
                    fileOutputStream.write(b, 0, readBytes);
                    fileOutputStream.write("cool beans".getBytes());
                }
                fileInputStream.close();
                fileOutputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

         */
    }
}
