package io.mapsmessaging.storage.impl.file.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.storage.impl.file.PartitionStorage;

public class PartitionStatisticsTask <T extends Storable> implements Runnable {

  private final PartitionStorage<T> storage;

  public PartitionStatisticsTask(PartitionStorage<T> storage) {
    this.storage = storage;
  }

  public void run() {
    StorageStatistics statistics = (StorageStatistics)storage.getStatistics();
    long readTimes = statistics.getReadLatency();
    long writeTimes = statistics.getWriteLatency();
    double reads = statistics.getReads()/10.0;
    double writes = statistics.getWrites()/10.0;
    double deletes = statistics.getDeletes()/10.0;
    double bytesRead = statistics.getBytesRead()/10.0;
    double bytesWrite = statistics.getBytesWritten()/10.0;
    System.err.println("+-------------------------------------------+");
    System.err.println(" Read:\t"+reads+
        "\n Writes:\t"+writes+
        "\n Deletes:\t"+deletes+
        "\n Bytes Read:\t"+convertToMB(bytesRead)+
        "\n Bytes Write:\t"+convertToMB(bytesWrite)+
        "\n Read Time:\t"+readTimes+
        "ms\n Write Times:\t"+writeTimes+"ms");
  }

  private String convertToMB(double value){

    if(value > 1024 * 1024 * 1024){
      return (int)(value/(1024*1024*1024))+"GB";
    }
    else if(value > 1024 * 1024 ){
      return (int)(value/(1024*1024))+"MB";
    }
    else if(value > 1024 ){
      return (int)(value/1024)+"KB";
    }
    return value+"Bytes";
  }
}