package com.ulwx.tool.memmap;

import com.ulwx.tool.SnowflakeIdWorker;
import com.ulwx.tool.crc.CRC;
import com.ulwx.tool.crc.CRC.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryMappedFile {
    private static Logger log = LoggerFactory.getLogger(MemoryMappedFile.class);
    private static int MemShareMaxLen = 1024 * 1024 * 512;        //512M 开辟共享内存大小
    private int fsize = 0;                          //文件的实际大小
    private String shareFileName;                   //共享内存文件名
    private String shareDirPath;                     //共享内存目录路径
    private MappedByteBuffer mapBuf = null;         //定义共享内存缓冲区
    private FileChannel fc = null;                  //RAFile文件通道
    private RandomAccessFile RAFile = null;         //定义一个随机存取文件对象
    private String RAFileName;

    private final static int PACKET_HEADER_LEN = 15;//类型（1），长度（4），crc(2)，snowid(8)
    private final static int META_DATA_LEN = 17;//数据(start,end,type,time)
    private final int REDUCE_LIMIT = 200;//相差200个字节时，自动缩容

    private int lastWriteIndexStart = -1;  //最后写入开始位置
    private int lastWriteIndexEnd = -1;  //最后写入结束位置
    private DataType lastWriteType = DataType.UNSUPPORT;
    private Date lastWriteTime;

    private int lastReadIndexStart = -1;  //最后读开始位置
    private int lastReadIndexEnd = -1;  //最后读结束位置
    private DataType lastReadType = DataType.UNSUPPORT;
    private Date lastReadTime;

    private RandomAccessFile RRAFileMeta = null;
    private MappedByteBuffer rmapBuf = null;
    private int rmapBufNextWritePos = 0;
    private String RRAFileMetaFileName;
    private RandomAccessFile WRAFileMeta = null;
    private MappedByteBuffer wmapBuf = null;
    private int wmapBufNextWritePos = 0;
    private String WRAFileMetaFileName;
    private final int META_MAP_FILE_LEN = 32 * 500;//500条记录

    private boolean expanding = false;//表示内存映射文件已写满，后面的写到临时文件

    private boolean closed = false;
    private String transStatusLogFileName;
    private RandomAccessFile transStatusLog = null;
    private FileChannel transStatusLogChannel = null;
    private FileLock fileLock;

    private Lock lock = new ReentrantLock(true);

    private static enum TransStatus {
        TRANS_OK(0),
        DATA_COPY_START(2000);
        private int value;

        public int getValue() {
            return value;
        }

        TransStatus(int value) {
            this.value = value;
        }
    }

    private TransStatus transStatus = TransStatus.TRANS_OK; //0 :事务正常结束   20001：缩容copy数据时读写指针调整开始

    /**
     * @param shareDirPath 共享内存文件路径
     * @param sf           内存映射文件名
     */
    public MemoryMappedFile(String shareDirPath, String sf, Integer initSize) throws Exception {

        init(shareDirPath, sf, initSize);

    }

    private void init(String shareDirPath, String sf, Integer initSize) throws Exception {

        if (!this.closed) {
            this.close();
        }

        if (initSize != null) {
            MemShareMaxLen = initSize;
        }
        if (shareDirPath.length() != 0) {
            new File(shareDirPath).mkdirs();
            this.shareDirPath = shareDirPath + File.separator;
        } else {
            this.shareDirPath = shareDirPath;
        }
        this.shareFileName = sf;

        this.transStatusLogFileName = this.shareDirPath + "trans.log";
        transStatusLog = new RandomAccessFile(transStatusLogFileName, "rws");
        transStatusLogChannel = transStatusLog.getChannel();
        fileLock = transStatusLogChannel.tryLock();
        if(fileLock==null){
            throw new Exception("程序已打开，不能再次打开！");
        }
        this.RAFileName = this.shareDirPath + this.shareFileName;
        this.RRAFileMetaFileName = this.shareDirPath + this.shareFileName + ".rmeta";
        this.WRAFileMetaFileName = this.shareDirPath + this.shareFileName + ".wmeta";

        RRAFileMeta = new RandomAccessFile(RRAFileMetaFileName, "rw");
        WRAFileMeta = new RandomAccessFile(WRAFileMetaFileName, "rw");
        if (RRAFileMeta.length() == 0 || WRAFileMeta.length() == 0) {
            ByteBuffer buffer = ByteBuffer.allocate(META_MAP_FILE_LEN);
            if (RRAFileMeta.length() == 0) {
                RRAFileMeta.write(buffer.array());
                RRAFileMeta.getFD().sync();
            }
            if (WRAFileMeta.length() == 0) {
                WRAFileMeta.write(buffer.array());
                WRAFileMeta.getFD().sync();
            }
        }

        RAFile = new RandomAccessFile(RAFileName, "rw");
        fc = RAFile.getChannel();
        //获取实际文件的大小
        fsize = (int) fc.size();

        if (fsize < MemShareMaxLen) {
            byte bb[] = new byte[(int) (MemShareMaxLen - fsize)];
            //创建字节缓冲区
            ByteBuffer bf = ByteBuffer.wrap(bb);
            ((Buffer)bf).clear();
            //设置此通道的文件位置。
            fc.position(fsize);
            //将字节序列从给定的缓冲区写入此通道。
            fc.write(bf);
            fc.force(false);
            fsize = MemShareMaxLen;

        }
        //将此通道的文件区域直接映射到内存中。
        mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fsize);
        rmapBuf = RRAFileMeta.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, META_MAP_FILE_LEN);
        wmapBuf = WRAFileMeta.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, META_MAP_FILE_LEN);

        this.handTransForDataCopyForRecoverFromFile();

        List<ReadMetaInfo> readMetaInfoList = new ArrayList<>();
        List<WriteMetaInfo> writeMetaInfoList = new ArrayList<>();
        this.readLastWritePosition(writeMetaInfoList);
        this.readLastReadPosition(readMetaInfoList);

        checkDataIntegrity(writeMetaInfoList);
        this.closed = false;
        this.expanding = false;


    }

    private  int getLastReadIndexEnd() {
        return this.lastReadIndexEnd;
    }

    private  int getLastReadIndexStart() {
        return lastReadIndexStart;
    }

    private DataType getLastReadType() {
        return lastReadType;
    }

    private  int getLastWriteIndexEnd() {
        return this.lastWriteIndexEnd;
    }

    private  int getLastWriteIndexStart() {
        return lastWriteIndexStart;
    }


    private  DataType getLastWriteType() {
        return lastWriteType;
    }

    private void checkDataIntegrity(List<WriteMetaInfo> writeMetaInfolist) throws Exception {
        Collections.sort(writeMetaInfolist);

        for (int i = writeMetaInfolist.size() - 1; i >= 0; i--) {
            WriteMetaInfo writeMetaInfo = writeMetaInfolist.get(i);
            try {
                FData fData = null;
                if (writeMetaInfo.lastWriteType == DataType.META_FOR_DATA_COPY) {
                    if (writeMetaInfo.lastWriteIndexStart == -1 && writeMetaInfo.lastWriteIndexEnd == -1) {
                        this.setLastReadPosition(writeMetaInfo.lastWriteIndexStart,
                                writeMetaInfo.lastWriteIndexEnd, DataType.META_FOR_DATA_FIXED);
                        this.setLastWritePosition(writeMetaInfo.lastWriteIndexStart,
                                writeMetaInfo.lastWriteIndexEnd, DataType.META_FOR_DATA_FIXED);

                    } else {
                        int startPos = writeMetaInfo.lastWriteIndexStart;
                        int end = writeMetaInfo.lastWriteIndexEnd;
                        FData oldfData = null;
                        try {

                            while (true) {
                                fData = this.read(startPos);
                                if (fData.getEnd() >= end) {
                                    break;
                                }
                                oldfData = fData;
                                startPos = fData.getEnd() + 1;
                            }
                        } catch (Exception ex) {
                            if (ex instanceof MMException) {
                                MMException mmException = (MMException) ex;
                                if (mmException.getErrorCode() == MMException.ERROR_CODE_NOT_SUPPORT ||
                                        mmException.getErrorCode() == MMException.ERROR_CODE_CRC_ERROR) {
                                    if (oldfData != null) {
                                        log.error("数据不完整，进行修复");
                                        this.setLastReadPosition(oldfData.getStart(),
                                                oldfData.getEnd(), DataType.META_FOR_DATA_FIXED);
                                        this.setLastWritePosition(oldfData.getStart(),
                                                oldfData.getEnd(), DataType.META_FOR_DATA_FIXED);
                                        log.error("数据不完整，修复完毕");
                                    }
                                }
                            }
                        }
                    }
                } else {
                    fData = this.read(writeMetaInfo.lastWriteIndexStart);
                }
                //此为最后一个写
                if (this.getLastWriteIndexStart() == writeMetaInfo.lastWriteIndexStart) {
                    /////
                } else {
                    log.error("读指针大于写指针，读写位置状态不一致，进行修复！");
                    if (this.getLastReadIndexEnd() > writeMetaInfo.lastWriteIndexEnd) {
                        this.setLastReadPosition(writeMetaInfo.lastWriteIndexStart,
                                writeMetaInfo.lastWriteIndexEnd, DataType.META_FOR_DATA_FIXED);

                    }
                    this.setLastWritePosition(writeMetaInfo.lastWriteIndexStart,
                            writeMetaInfo.lastWriteIndexEnd, DataType.META_FOR_DATA_FIXED);
                    log.error("读指针大于写指针，读写位置状态不一致，完成修复！");

                }
                break;
            } catch (Exception e2) {
                log.error("" + e2, e2);
            }
        }

        if (this.getLastReadIndexEnd() > this.getLastWriteIndexEnd()) {
            this.setLastReadPosition(this.getLastReadIndexEnd(),
                    this.getLastWriteIndexEnd(), DataType.META_FOR_DATA_FIXED);
            log.error("读指针大于写指针，读写位置状态不一致，已修复！");
        }

    }

    private void setLastReadPosition(int startpos, int endpos, DataType dt) throws Exception {
        this.lastReadIndexEnd = endpos;
        this.lastReadIndexStart = startpos;
        this.lastReadType = dt;
        this.lastReadTime = new Date();
        byte[] bytes = getMeta(startpos, endpos, dt, this.lastReadTime);

        ((Buffer)rmapBuf).position(this.rmapBufNextWritePos);
        rmapBuf.put(bytes);
        this.rmapBufNextWritePos = (this.rmapBufNextWritePos + PACKET_HEADER_LEN + META_DATA_LEN) % META_MAP_FILE_LEN;

    }

    private void readLastReadPosition(List<ReadMetaInfo> list) throws Exception {
        ReadMetaInfo readMetaInfo = readLastReadPosition(RRAFileMeta, this.RRAFileMetaFileName, list);
        this.lastReadIndexStart = readMetaInfo.lastReadIndexStart;
        this.lastReadIndexEnd = readMetaInfo.lastReadIndexEnd;
        this.lastReadType = readMetaInfo.lastReadType;
        this.lastReadTime = readMetaInfo.lastReadTime;
        if (readMetaInfo.metaPos >= 0) {
            this.rmapBufNextWritePos = (readMetaInfo.metaPos + PACKET_HEADER_LEN + META_DATA_LEN) % META_MAP_FILE_LEN;
        } else {
            this.rmapBufNextWritePos = 0;
        }
    }


    private static ReadMetaInfo readLastReadPosition(RandomAccessFile RRAFileMeta, String fileName,
                                                    List<ReadMetaInfo> readMetaInfoList) throws Exception {

        byte[] bs = new byte[PACKET_HEADER_LEN + META_DATA_LEN];
        boolean suc = true;
        int postion = 0;
        while (postion < RRAFileMeta.length()) {
            RRAFileMeta.seek(postion);
            RRAFileMeta.readFully(bs);
            ByteBuffer bb = ByteBuffer.wrap(bs);
            //类型（1），长度（4），crc(2)，数据(start,end,type,time)
            byte type = bb.get();
            if (DataType.valueOf(type) == DataType.UNSUPPORT) {
                break;
            }
            int len = bb.getInt();
            short crc = bb.getShort();
            long id = bb.getLong();
            byte[] data = new byte[META_DATA_LEN];
            bb.get(data);
            short dataCrc = (short) CRC.calculateCRC(Parameters.CRC16, data);
            if (crc == dataCrc) {
                ByteBuffer dataBuffer = ByteBuffer.wrap(data);
                ReadMetaInfo readMetaInfo = new ReadMetaInfo();
                readMetaInfo.lastReadIndexStart = dataBuffer.getInt();
                readMetaInfo.lastReadIndexEnd = dataBuffer.getInt();
                readMetaInfo.lastReadType = DataType.valueOf(dataBuffer.get());
                readMetaInfo.lastReadTime = new Date(dataBuffer.getLong());
                readMetaInfo.fileName = fileName;
                readMetaInfo.id = id;
                readMetaInfo.metaPos = postion;
                readMetaInfoList.add(readMetaInfo);
                suc = true;

            } else {
                suc = false;
                log.error("记录的读指针信息不完整，试图恢复读指针！");
                break;
            }
            postion = postion + PACKET_HEADER_LEN + META_DATA_LEN;
        }
        Collections.sort(readMetaInfoList);
        if (readMetaInfoList.size() > 0) {
            return readMetaInfoList.get(readMetaInfoList.size() - 1);
        } else {
            ReadMetaInfo readMetaInfo = new ReadMetaInfo();
            readMetaInfo.lastReadIndexStart = -1;
            readMetaInfo.lastReadIndexEnd = -1;
            readMetaInfo.lastReadTime = null;
            readMetaInfo.id = -1;
            readMetaInfo.lastReadType = DataType.UNSUPPORT;
            readMetaInfo.metaPos = -1;
            return readMetaInfo;
        }

    }


    private void setLastWritePosition(int startpos, int endpos, DataType dt) throws Exception {
        this.lastWriteIndexStart = startpos;
        this.lastWriteIndexEnd = endpos;
        this.lastWriteType = dt;
        this.lastWriteTime = new Date();


        byte[] bytes = getMeta(startpos, endpos, dt, this.lastWriteTime);
        ((Buffer)wmapBuf).position(this.wmapBufNextWritePos);
        wmapBuf.put(bytes);
        this.wmapBufNextWritePos = (this.wmapBufNextWritePos + PACKET_HEADER_LEN + META_DATA_LEN) % META_MAP_FILE_LEN;

    }

    private static WriteMetaInfo readLastWritePosition(RandomAccessFile WRAFileMeta, String fileName,
                                                      List<WriteMetaInfo> writeMetaInfoList) throws Exception {

        byte[] bs = new byte[PACKET_HEADER_LEN + META_DATA_LEN];
        boolean suc = true;

        int postion = 0;
        while (postion < WRAFileMeta.length()) {
            WRAFileMeta.seek(postion);
            WRAFileMeta.readFully(bs);
            ByteBuffer bb = ByteBuffer.wrap(bs);
            //类型（1），长度（4），crc(2)，id(8),数据(start,end,type,time)
            byte type = bb.get();
            if (DataType.valueOf(type) == DataType.UNSUPPORT) {
                break;
            }
            int len = bb.getInt();
            short crc = bb.getShort();
            long id = bb.getLong();
            byte[] data = new byte[META_DATA_LEN];
            bb.get(data);
            short dataCrc = (short) CRC.calculateCRC(Parameters.CRC16, data);
            if (crc == dataCrc) {
                WriteMetaInfo writeMetaInfo = new WriteMetaInfo();
                ByteBuffer dataBuffer = ByteBuffer.wrap(data);
                writeMetaInfo.lastWriteIndexStart = dataBuffer.getInt();
                writeMetaInfo.lastWriteIndexEnd = dataBuffer.getInt();
                writeMetaInfo.lastWriteType = DataType.valueOf(dataBuffer.get());
                writeMetaInfo.lastWriteTime = new Date(dataBuffer.getLong());
                writeMetaInfo.id = id;
                writeMetaInfo.fileName = fileName;
                writeMetaInfo.metaPos = postion;
                writeMetaInfoList.add(writeMetaInfo);
                suc = true;

            } else {
                suc = false;
                log.error("记录的读指针信息不完整，试图恢复读指针！");
                break;
            }
            postion = postion + PACKET_HEADER_LEN + META_DATA_LEN;
        }
        if (writeMetaInfoList.size() > 0) {
            Collections.sort(writeMetaInfoList);
            return writeMetaInfoList.get(writeMetaInfoList.size() - 1);
        } else {
            WriteMetaInfo writeMetaInfo = new WriteMetaInfo();
            writeMetaInfo.lastWriteIndexStart = -1;
            writeMetaInfo.lastWriteIndexEnd = -1;
            writeMetaInfo.lastWriteTime = null;
            writeMetaInfo.id = -1;
            writeMetaInfo.lastWriteType = DataType.UNSUPPORT;
            writeMetaInfo.metaPos = -1;
            return writeMetaInfo;
        }


    }

    private void readLastWritePosition(List<WriteMetaInfo> list) throws Exception {
        WriteMetaInfo writeMetaInfo = readLastWritePosition(WRAFileMeta, this.WRAFileMetaFileName, list);
        this.lastWriteIndexStart = writeMetaInfo.lastWriteIndexStart;
        this.lastWriteIndexEnd = writeMetaInfo.lastWriteIndexEnd;
        this.lastWriteType = writeMetaInfo.lastWriteType;
        this.lastWriteTime = writeMetaInfo.lastWriteTime;
        if (writeMetaInfo.metaPos >= 0) {
            this.wmapBufNextWritePos = (writeMetaInfo.metaPos + PACKET_HEADER_LEN + META_DATA_LEN) % META_MAP_FILE_LEN;
        } else {
            this.wmapBufNextWritePos = 0;
        }
    }

    private static byte[] getMeta(int start, int end, DataType dt, Date date) {
        //类型（1），长度（4），crc(2)，数据(start,end,type,time)
        ByteBuffer bb = ByteBuffer.allocate(PACKET_HEADER_LEN + META_DATA_LEN);
        bb.put(DataType.META_FOR_COMMON.getValue());//类型

        ByteBuffer data = ByteBuffer.allocate(META_DATA_LEN);
        data.putInt(start);
        data.putInt(end);
        data.put(dt.getValue());
        data.putLong(date.getTime());
        byte[] databs = data.array();
        bb.putInt(META_DATA_LEN);//数据长度
        bb.putShort((short) CRC.calculateCRC(Parameters.CRC16, databs));//crc
        bb.putLong(SnowflakeIdWorker.instance.nextId());//id
        bb.put(databs);//数据
        return bb.array();
    }

    private void handTransForDataCopy(int length) throws Exception {

        transStatus = TransStatus.DATA_COPY_START;
        String str = transStatus.getValue() + ":" + length + ":over";
        transStatusLog.write(str.getBytes("utf-8"));
        this.setLastReadPosition(-1, -1, DataType.META_FOR_DATA_COPY);
        if (length > 0) {
            this.setLastWritePosition(0, length - 1, DataType.META_FOR_DATA_COPY);
        } else {
            this.setLastWritePosition(-1, -1, DataType.META_FOR_DATA_COPY);
        }
        transStatusLog.setLength(0);
        transStatus = TransStatus.TRANS_OK;
    }

    private void handTransForDataCopyForRecoverFromFile() throws Exception {
        if (transStatusLog.length() > 0) {
            byte[] bs = new byte[(int) transStatusLog.length()];
            transStatusLog.readFully(bs);
            String s = new String(bs, "utf-8");
            if (s != null && !s.isEmpty()) {
                String[] strs = s.split(":");
                if (Integer.valueOf(strs[0]).intValue() == TransStatus.DATA_COPY_START.getValue()) {
                    if (strs[2].equals("over")) {
                        int len = Integer.valueOf(strs[1]);
                        if (len >= 0) {
                            handTransForDataCopy(len);
                        }

                    }
                }
            }
        }

    }

    /**
     * @param buff 写入的数据
     * @return
     */
    private void write(byte[] buff, DataType dt) throws Exception {

        int postion = this.getLastWriteIndexEnd() + 1;
        if (this.transStatus == TransStatus.DATA_COPY_START) {
            handTransForDataCopyForRecoverFromFile();
            postion = this.getLastWriteIndexEnd() + 1;
        }
        int len = buff.length + PACKET_HEADER_LEN;
        if (postion > fsize - 1 || postion + len > fsize) {
            int delta = this.getLastWriteIndexEnd() - this.getLastReadIndexEnd();
            if (delta < REDUCE_LIMIT && (delta - 1) <= this.getLastReadIndexStart()) { //进行缩容
                //拷贝
                byte[] copyBytes = new byte[delta];
                ((Buffer)this.mapBuf).position(this.getLastReadIndexEnd() + 1);
                this.mapBuf.get(copyBytes);
                ((Buffer)this.mapBuf).position(0);
                this.mapBuf.put(copyBytes);
                handTransForDataCopy(copyBytes.length);
                postion = this.getLastWriteIndexEnd() + 1;
            } else {
                if (!this.expanding) {
                    this.expanding = true;
                    int extenSize = this.fsize + this.fsize / 5;
                    if (extenSize < (postion + len)) {
                        extenSize = extenSize + this.fsize / 5 + ((postion + len) - extenSize);
                    }
                    this.init(this.shareDirPath, this.shareFileName, extenSize);
                    postion = this.getLastWriteIndexEnd() + 1;

                }
            }
        }

        if (postion <= getLastReadIndexEnd()) {
            throw new Exception("写位置必须大于读位置！");
        }

        try {
            ((Buffer)mapBuf).position(postion);
            //写入类型
            mapBuf.put(dt.getValue());
            //写入长度
            mapBuf.putInt(buff.length);
            //crc
            short crc16 = (short) CRC.calculateCRC(Parameters.CRC16, buff);
            mapBuf.putShort(crc16);
            mapBuf.putLong(SnowflakeIdWorker.instance.nextId());//id
            //数据
            mapBuf.put(buff);
            //更新写位置
            this.setLastWritePosition(postion, postion + buff.length + PACKET_HEADER_LEN - 1, dt);

        } catch (Exception e) {
            throw e;
        }

    }


    public synchronized void writeString(String str) throws Exception {
        lock.lock();
        try {
            byte[] bs = str.getBytes("utf-8");
            write(bs, DataType.STRING);
        }finally{
            lock.unlock();
        }

    }


    public  void readString(LogicFun lf) throws Exception {
        lock.lock();
        try {
            int postion = this.getLastReadIndexEnd() + 1;
            FData fdata = this.read(postion);
            String str = new String(fdata.getData(), "utf-8");
            lf.call(str,1);
            this.setLastReadPosition(fdata.getStart(), fdata.getEnd(), DataType.STRING);
        }finally {
            lock.unlock();
        }
    }

    public  void readNString(int n,LogicFun lf,String delimiter) throws Exception {
        lock.lock();
        try {
            StringBuffer stringBuffer=new StringBuffer();
            FData fdata=null;
            int postion = this.getLastReadIndexEnd() + 1;
            int count=0;
            for(int i=0; i<n; i++){
                try {
                   fdata = this.read(postion);
                    String str = new String(fdata.getData(), "utf-8");
                    if(stringBuffer.length()>0) {
                        stringBuffer.append(delimiter);
                    }
                    stringBuffer.append(str);

                    postion=fdata.getEnd()+1;
                    count++;
                }catch (Exception e){
                    if(e instanceof MMException){
                        MMException e2= (MMException) e;
                        if(e2.getErrorCode()==MMException.ERROR_CODE_READ_OVER){
                            break;
                        }
                        throw e;
                    }
                }
            }

            lf.call(stringBuffer.toString(),count);
            if(fdata!=null) {
                this.setLastReadPosition(fdata.getStart(), fdata.getEnd(), DataType.STRING);
            }
        }finally {
            lock.unlock();
        }
    }


    public  String readString() throws Exception {
        lock.lock();
        try {
            int postion = this.getLastReadIndexEnd() + 1;
            FData fdata = this.read(postion);
            if (fdata.getDt() != DataType.STRING) {
                throw new Exception("实际存储的不为String类型，请核实！");
            }
            String str = new String(fdata.getData(), "utf-8");
            this.setLastReadPosition(fdata.getStart(), fdata.getEnd(), DataType.STRING);
            return str;
        } finally {
            lock.unlock();
        }

    }


    private  WriteMetaInfo getWriteMetaInfo() throws Exception {
        WriteMetaInfo metaInfo = new WriteMetaInfo();

        metaInfo.lastWriteIndexStart = this.lastWriteIndexStart;
        metaInfo.lastWriteIndexEnd = this.lastWriteIndexEnd;
        metaInfo.lastWriteType = this.lastWriteType;
        metaInfo.lastWriteTime = this.lastWriteTime;
        metaInfo.fileName = this.shareFileName + ".wmeta";
        return metaInfo;
    }


    private FData read(int postion) throws Exception {
        try {
            if (this.transStatus == TransStatus.DATA_COPY_START) {
                handTransForDataCopyForRecoverFromFile();
                postion = this.getLastReadIndexEnd() + 1;
            }
            if (postion > this.getLastWriteIndexEnd()) {
                throw new MMException(MMException.ERROR_CODE_READ_OVER,
                        "写的内容已被读完！postion:" + postion);
            }
            ////类型（1），长度（4），crc(2)，数据()
            ((Buffer)mapBuf).position(postion);
            byte type = mapBuf.get();
            DataType dt = DataType.valueOf(type);
            if (dt == DataType.UNSUPPORT) {
                throw new MMException(MMException.ERROR_CODE_NOT_SUPPORT, "不支持的数据类型！");
            }
            int len = mapBuf.getInt();
            short crc = mapBuf.getShort();
            long id = mapBuf.getLong();
            FData fData = new FData();
            if (len > 0) {
                byte[] bb = new byte[len];
                mapBuf.get(bb);
                short dataCrc = (short) CRC.calculateCRC(Parameters.CRC16, bb);
                if (crc != dataCrc) {
                    throw new MMException(MMException.ERROR_CODE_CRC_ERROR, "crc校验出错，数据不完整！位置[" + postion + "]");
                }
                fData.setData(bb);
            } else if (len == 0) {
                fData.setData(new byte[0]);
            } else {
                throw new Exception("数据长度为负数！");
            }
            fData.setDt(dt);
            fData.setStart(postion);
            fData.setEnd(postion + fData.getData().length + this.PACKET_HEADER_LEN - 1);
            return fData;
        } catch (Exception e) {
            throw e;
        }

    }

    private void clearFiles() throws Exception {
        if (!closed) {
            close();
        }
        new File(this.RAFileName).delete();
        new File(this.RRAFileMetaFileName).delete();
        new File(this.WRAFileMetaFileName).delete();
    }

    /**
     * 关闭内存映射
     */
    private  void close() throws Exception {
        if (rmapBuf != null) {
            try {
                rmapBuf.force();
                rmapBuf = null;
            } catch (Exception e) {
                throw e;
            }
        }
        if (wmapBuf != null) {
            try {
                wmapBuf.force();
                wmapBuf = null;
            } catch (Exception e) {
                throw e;
            }
        }

        if (WRAFileMeta != null) {
            try {
                WRAFileMeta.close();
                WRAFileMeta = null;
            } catch (Exception e) {
                throw e;
            }

        }
        if (RRAFileMeta != null) {
            try {
                RRAFileMeta.close();
                RRAFileMeta = null;
            } catch (Exception e) {
                throw e;
            }

        }

        if (mapBuf != null) {
            try {
                mapBuf.force();
                mapBuf = null;
            } catch (Exception e) {
                throw e;
            }
        }
        if (fc != null) {
            try {
                fc.force(false);
                fc.close();
                fc = null;
            } catch (Exception e) {
                throw e;
            }

        }

        if (RAFile != null) {
            try {
                RAFile.close();
                RAFile = null;
                System.gc();
            } catch (Exception e) {
                throw e;
            }

        }
        if (transStatusLogChannel != null) {
            fileLock.release();
            transStatusLogChannel.close();
            transStatusLogChannel = null;

        }
        if (transStatusLog != null) {
            try {
                transStatusLog.close();
                transStatusLog = null;
            } catch (Exception e) {
                throw e;
            }
        }

        closed = true;

    }


    public static void main(String arsg[]) throws Exception {
        MemoryMappedFile sm = new MemoryMappedFile("D:\\demo", "test.txt", 100);

        sm.writeString("中文测试1");
        sm.writeString("中文测试2");

        sm.writeString("中文测试3");
        sm.writeString("中文测试4");
        System.out.println(sm.readString());
        System.out.println(sm.readString());
        sm.writeString("中文测试5");
        System.out.println(sm.readString());
        System.out.println(sm.readString());
        System.out.println(sm.readString());
        sm.writeString("中文测试6");
        System.out.println(sm.readString());
        sm.writeString("中文测试7");
        sm.writeString("中文测试8");

        sm.readNString(100,(str,n)->{
            System.out.println(str+",count="+n);
        },",");

        // AtomicInteger ai=new AtomicInteger();
        // ai.set(0);
        // new Thread(){
        //     @Override
        //     public void run() {
        //         try {
        //             while(true) {
        //                 sm.writeString("中文测试" + ai.incrementAndGet());
        //             }
        //         } catch (Exception e) {
        //             e.printStackTrace();
        //         }
        //     }
        // }.start();
        //
        // new Thread(){
        //     @Override
        //     public void run() {
        //         try {
        //             while(true) {
        //                 sm.writeString("中文测试" + ai.incrementAndGet());
        //             }
        //         } catch (Exception e) {
        //             e.printStackTrace();
        //         }
        //     }
        // }.start();
        //     Thread.sleep(3000);
        //     new Thread(){
        //         @Override
        //         public void run() {
        //             try {
        //                 while(true) {
        //                     System.out.println(sm.readString());
        //                 }
        //             } catch (Exception e) {
        //                 e.printStackTrace();
        //             }
        //         }
        //     }.start();
        //     new Thread(){
        //         @Override
        //         public void run() {
        //             try {
        //                 while(true) {
        //                     System.out.println(sm.readString());
        //                 }
        //             } catch (Exception e) {
        //                 e.printStackTrace();
        //             }
        //         }
        //     }.start();
    }
}

