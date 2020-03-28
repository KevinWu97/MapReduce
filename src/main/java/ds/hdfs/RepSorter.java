package ds.hdfs;

import proto.ProtoHDFS;

import java.util.Comparator;

// Sorts replications of blocks based on its replication number
public class RepSorter implements Comparator<ProtoHDFS.Block> {
    @Override
    public int compare(ProtoHDFS.Block b1, ProtoHDFS.Block b2) {
        int compVal = 0;
        if(b1.getBlockMeta().getRepNumber() < b2.getBlockMeta().getRepNumber()){
            compVal = -1;
        }else if(b1.getBlockMeta().getRepNumber() > b2.getBlockMeta().getRepNumber()){
            compVal = 1;
        }
        return compVal;
    }
}
