package com.ulwx.tool.memmap;

import java.util.Date;

public  class ReadMetaInfo implements Comparable<ReadMetaInfo>{

    public int metaPos=-1;
    public int lastReadIndexStart = -1;  //最后读开始位置
    public int lastReadIndexEnd = -1;  //最后读结束位置
    public DataType lastReadType = DataType.UNSUPPORT;
    public Date lastReadTime;
    public String fileName;
    public  long id;
    public String toString() {
        return "metaPos:"+metaPos+",lastReadIndexStart:" + lastReadIndexStart + ",lastReadIndexEnd:" +
                lastReadIndexEnd + ",lastReadType:" + lastReadType + ",fileName:"+fileName;
    }

    @Override
    public int compareTo(ReadMetaInfo o) {
        if(this.id<o.id) return -1;
        else if(this.id==o.id) return 0;
        else return 1;
    }
}