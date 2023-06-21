package com.ulwx.tool.memmap;

public  enum DataType {
    UNSUPPORT(0),
    INT(1),
    LONG(2),
    FLOAT(3),
    DOUBLE(4),
    STRING(5),
    META_FOR_COMMON(10),//数据有17位,结构为 start(4),end(4),datatype(1),time(8)
    META_FOR_DATA_FIXED(11),//数据
    META_FOR_DATA_COPY(12);

    private byte value;

    DataType(byte value) {
        this.value = value;
    }

    DataType(int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }

    public static DataType valueOf(int value) {
        switch (value) {
            case 1:
                return INT;
            case 2:
                return LONG;
            case 3:
                return FLOAT;
            case 4:
                return DOUBLE;
            case 5:
                return STRING;
            case 10:
                return META_FOR_COMMON;
            case 11:
                 return META_FOR_DATA_FIXED;
            case 12:
                return META_FOR_DATA_COPY;
            default:
                return UNSUPPORT;
        }
    }
}