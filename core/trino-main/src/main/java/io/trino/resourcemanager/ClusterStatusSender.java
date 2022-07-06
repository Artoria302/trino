package io.trino.resourcemanager;

import io.trino.execution.ManagedQueryExecution;

public interface ClusterStatusSender
{
    void registerQuery(ManagedQueryExecution queryExecution);
}
