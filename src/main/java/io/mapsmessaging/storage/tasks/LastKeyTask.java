package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class LastKeyTask <T extends Storable> extends BaseTask<T, Long> {

    public LastKeyTask(@NotNull Storage<T> storage) {
        super(storage, null);
    }

    @Override
    public Long execute() throws Exception {
        return storage.getLastKey();
    }
}
