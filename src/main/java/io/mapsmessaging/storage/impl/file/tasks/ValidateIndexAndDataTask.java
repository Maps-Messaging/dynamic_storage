package io.mapsmessaging.storage.impl.file.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.ManagedStorage;

public class ValidateIndexAndDataTask<N, T extends Storable> implements FileTask<N>{

  private final ManagedStorage<T> storeToValidate;
  private long index;

  public ValidateIndexAndDataTask(ManagedStorage<T> storeToValidate){
    this.storeToValidate = storeToValidate;
    index = 0;
  }

  @Override
  public N call() throws Exception {

    return null;
  }
}
