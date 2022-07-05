package io.trino.resourcemanager;

public class ResourceManagerInconsistentException
        extends RuntimeException
{
    public ResourceManagerInconsistentException(String message)
    {
        super(message);
    }

    @Override
    public String getMessage()
    {
        return super.getMessage();
    }
}
