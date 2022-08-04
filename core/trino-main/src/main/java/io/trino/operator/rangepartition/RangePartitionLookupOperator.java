/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.rangepartition;

import io.trino.operator.PageBuffer;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.trino.spi.Page;

import static io.trino.operator.WorkProcessor.flatten;

public class RangePartitionLookupOperator
        implements AdapterWorkProcessorOperator
{
    private final boolean waitForBuild;
    private final PageBuffer pageBuffer;
    private final RangePartitionLookupProcessor lookupProcessor;
    private final WorkProcessor<Page> pages;

    RangePartitionLookupOperator(boolean waitForBuild)
    {
        this.waitForBuild = waitForBuild;
        pageBuffer = new PageBuffer();
        lookupProcessor = new RangePartitionLookupProcessor();
        pages = flatten(WorkProcessor.create(lookupProcessor));
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return null;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
    }

    @Override
    public void finish()
    {
    }
}
