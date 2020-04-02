package ds.hdfs;

import com.google.protobuf.InvalidProtocolBufferException;
import proto.ProtoHDFS;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SendHeartbeatBlockReportTask implements Runnable {
    DataNode dataNode;
    String nameNodeId;
    String nameNodeIp;
    int namePort;

    public SendHeartbeatBlockReportTask(String nameNodeId, String nameNodeIp, int nameNodePort, DataNode dataNode){
        this.nameNodeId = nameNodeId;
        this.nameNodeIp = nameNodeIp;
        this.namePort = nameNodePort;
        this.dataNode = dataNode;
    }

    @Override
    public void run() {
        // Connect data node to the name node and start sending heartbeats and block reports
        NameNodeInterface nameStub = dataNode.getNNStub(nameNodeId, nameNodeIp, namePort);

        // Building the heartbeat
        ProtoHDFS.NodeMeta.Builder nodeMetaBuilder = ProtoHDFS.NodeMeta.newBuilder();
        nodeMetaBuilder.setId(dataNode.dataId);
        nodeMetaBuilder.setIp(dataNode.dataIp);
        nodeMetaBuilder.setPort(dataNode.dataPort);
        ProtoHDFS.NodeMeta nodeMeta = nodeMetaBuilder.build();
        nodeMetaBuilder.clear();

        ProtoHDFS.Heartbeat.Builder heartBeatBuilder = ProtoHDFS.Heartbeat.newBuilder();
        heartBeatBuilder.setDataNodeMeta(nodeMeta);
        ProtoHDFS.Heartbeat heartbeat = heartBeatBuilder.build();
        heartBeatBuilder.clear();

        // Building the block report
        List<String> blockMetaKeys = Collections.list(dataNode.blockMetas.keys());
        ArrayList<ProtoHDFS.BlockMeta> blockMetas = new ArrayList<>();
        for (String b : blockMetaKeys) {
            blockMetas.add(dataNode.blockMetas.get(b));
        }

        ProtoHDFS.BlockReport.Builder blockReportBuilder = ProtoHDFS.BlockReport.newBuilder();
        blockReportBuilder.setDataId(dataNode.dataId);
        blockReportBuilder.addAllDataNodeBlocks(blockMetas);
        ProtoHDFS.BlockReport blockReport = blockReportBuilder.build();
        blockReportBuilder.clear();

        try {
            nameStub.heartBeat(heartbeat.toByteArray());
            nameStub.blockReport(blockReport.toByteArray());
        } catch (RemoteException | InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}
