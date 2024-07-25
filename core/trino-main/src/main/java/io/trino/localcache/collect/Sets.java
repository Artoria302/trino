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

import com.google.common.collect.Iterators;
import com.google.common.collect.Multiset;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class Sets
{
    private Sets() {}

    abstract static class ImprovedAbstractSet<E extends @Nullable Object>
            extends AbstractSet<E>
    {
        @Override
        public boolean removeAll(Collection<?> c)
        {
            return removeAllImpl(this, c);
        }

        @Override
        public boolean retainAll(Collection<?> c)
        {
            return super.retainAll(requireNonNull(c)); // GWT compatibility
        }
    }

    static boolean removeAllImpl(Set<?> set, Collection<?> collection)
    {
        requireNonNull(collection); // for GWT
        if (collection instanceof Multiset) {
            collection = ((Multiset<?>) collection).elementSet();
        }
        /*
         * AbstractSet.removeAll(List) has quadratic behavior if the list size
         * is just more than the set's size.  We augment the test by
         * assuming that sets have fast contains() performance, and other
         * collections don't.  See
         * http://code.google.com/p/guava-libraries/issues/detail?id=1013
         */
        if (collection instanceof Set && collection.size() > set.size()) {
            return Iterators.removeAll(set.iterator(), collection);
        }
        else {
            return removeAllImpl(set, collection.iterator());
        }
    }

    static boolean removeAllImpl(Set<?> set, Iterator<?> iterator)
    {
        boolean changed = false;
        while (iterator.hasNext()) {
            changed |= set.remove(iterator.next());
        }
        return changed;
    }

    public static <E extends @Nullable Object> HashSet<E> newHashSetWithExpectedSize(
            int expectedSize)
    {
        return new HashSet<E>(Maps.capacity(expectedSize));
    }
}
