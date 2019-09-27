package io.milvus.client.params;

public enum IndexType {

    INVALID(0),
    FLAT(1),
    IVFLAT(2),
    IVF_SQ8(3),
    MIX_NSG(4);

    private final int indexType;

    IndexType(int indexType) {
        this.indexType = indexType;
    }

    public int getVal() {
        return indexType;
    }
}
