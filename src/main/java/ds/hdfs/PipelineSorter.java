package ds.hdfs;

import proto.ProtoHDFS;

import java.util.Comparator;
import java.util.List;

// Sorts pipelines based on the block number
public class PipelineSorter implements Comparator<ProtoHDFS.Pipeline> {
    @Override
    public int compare(ProtoHDFS.Pipeline p1, ProtoHDFS.Pipeline p2) {
        int compVal = 0;
        if(p1.getPipelineNumber() < p2.getPipelineNumber()){
            compVal = -1;
        }else if(p1.getPipelineNumber() > p2.getPipelineNumber()){
            compVal = 1;
        }
        return compVal;
    }
}
