package com.ulwx.tool.memmap;

public interface LogicFun<T> {
    /**
     * 如果方法抛出异常，则不会保存读指针
     * @param t
     * @throws Exception
     */
    public void call(T t,int count) throws Exception;
}
