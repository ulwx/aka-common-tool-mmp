package com.ulwx.tool.memmap;

public class MMException extends Exception{
   public static final int ERROR_CODE_READ_OVER =1000;
    public static final int ERROR_CODE_NOT_SUPPORT=1001;
    public static final int ERROR_CODE_CRC_ERROR=1002;
    private int errorCode;

    public MMException(int errorCode,String msg){
        super(msg);
        this.errorCode=errorCode;
    }
    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }
    @Override
    public String toString(){
        return "错误码["+errorCode+"]"+super.toString();
    }
}
