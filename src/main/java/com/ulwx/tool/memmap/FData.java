package com.ulwx.tool.memmap;

public class FData {
    private byte[] data;
    private DataType dt=DataType.UNSUPPORT;
    private int start;
    private int end;

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public DataType getDt() {
        return dt;
    }

    public void setDt(DataType dt) {
        this.dt = dt;
    }
}
