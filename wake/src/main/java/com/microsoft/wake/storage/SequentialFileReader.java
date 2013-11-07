package com.microsoft.wake.storage;

import java.io.FileInputStream;
import java.io.IOException;

import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;

public class SequentialFileReader implements EStage<ReadRequest> {
  final EventHandler<ReadResponse> dest = null;
  final FileHandlePool fdPool = new FileHandlePool();
  
  @Override
  public void onNext(ReadRequest value) {
    FileInputStream fin = fdPool.get(value.f);
    int readSoFar = 0;
    try {
      synchronized (fin) {
        fin.reset();
        fin.skip(value.offset);
        while(readSoFar != value.buf.length) {
          int ret = fin.read(value.buf, readSoFar, value.buf.length);
          if(ret == -1) { break; }
          readSoFar += ret;
        }
      }
    } catch(IOException e) {
      fdPool.release(value.f, fin);
//      err.onNext(null); //new ReadError(e));
    }
    fdPool.release(value.f, fin);
    dest.onNext(new ReadResponse(value.buf, readSoFar, value.id));
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
  }

}
