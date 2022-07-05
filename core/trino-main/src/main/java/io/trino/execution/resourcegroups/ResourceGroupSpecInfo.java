package io.trino.execution.resourcegroups;


public class ResourceGroupSpecInfo
{
    private final int softConcurrencyLimit;

    public ResourceGroupSpecInfo(int softConcurrencyLimit)
    {
        this.softConcurrencyLimit = softConcurrencyLimit;
    }

    public int getSoftConcurrencyLimit()
    {
        return softConcurrencyLimit;
    }
}
