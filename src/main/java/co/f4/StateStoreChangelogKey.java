package co.f4;

import java.io.Serializable;

public class StateStoreChangelogKey implements Serializable {
    private String storeName;
    private String storeKey;
    
    public StateStoreChangelogKey() {
    }
    
    public StateStoreChangelogKey(String storeName, String storeKey) {
        this.storeName = storeName;
        this.storeKey = storeKey;
    }

    public String getStoreName() { return storeName; }

    public String getStoreKey() { return storeKey; }
}
