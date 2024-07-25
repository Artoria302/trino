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
package io.trino.localcache.collect;

import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.UnmodifiableListIterator;
import com.google.common.primitives.Ints;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class Iterators
{
    private Iterators()
    {
    }

    static <T extends @Nullable Object> UnmodifiableIterator<T> emptyIterator()
    {
        return emptyListIterator();
    }

    static <T extends @Nullable Object> UnmodifiableListIterator<T> emptyListIterator()
    {
        return (UnmodifiableListIterator<T>) ArrayItr.EMPTY;
    }

    static void clear(Iterator<?> iterator)
    {
        requireNonNull(iterator);
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
    }

    private static final class ArrayItr<T extends @Nullable Object>
            extends AbstractIndexedListIterator<T>
    {
        static final UnmodifiableListIterator<Object> EMPTY = new ArrayItr<>(new Object[0], 0, 0, 0);

        private final T[] array;
        private final int offset;

        ArrayItr(T[] array, int offset, int length, int index)
        {
            super(length, index);
            this.array = array;
            this.offset = offset;
        }

        @Override
        @ParametricNullness
        protected T get(int index)
        {
            return array[offset + index];
        }
    }

    public static int size(Iterator<?> iterator)
    {
        long count = 0L;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return Ints.saturatedCast(count);
    }
}
