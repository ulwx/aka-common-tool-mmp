package com.ulwx.tool.memmap;

import java.util.Date;

public  class WriteMetaInfo implements Comparable<WriteMetaInfo>{
    public int metaPos=-1;
    public int lastWriteIndexStart = -1;  //最后写入开始位置
    public int lastWriteIndexEnd = -1;  //最后写入结束位置
    public DataType lastWriteType = DataType.UNSUPPORT;
    public long id=-1;
    public Date lastWriteTime;
    public String fileName;
    public String toString() {
        return "metaPos:" +metaPos+
                ",lastWriteIndexStart:" + lastWriteIndexStart + ",lastWriteIndexEnd:" + lastWriteIndexEnd
                + ",lastWriteType:" + lastWriteType+";fileName:"+fileName ;
    }

    @Override
    public int compareTo(WriteMetaInfo o) {
        if(this.id<o.id) return -1;
        else if(this.id==o.id) return 0;
        else return 1;
    }
}