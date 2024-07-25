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

import com.google.common.annotations.GwtCompatible;
import com.google.common.base.Function;
import com.google.common.primitives.Ints;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.j2objc.annotations.Weak;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import javax.annotation.CheckForNull;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@SuppressModernizer
public class Maps
{
    private Maps() {}

    private enum EntryFunction
            implements Function<Map.Entry<?, ?>, @Nullable Object>
    {
        KEY {
            @Override
            @CheckForNull
            public Object apply(Map.Entry<?, ?> entry)
            {
                return entry.getKey();
            }
        },
        VALUE {
            @Override
            @CheckForNull
            public Object apply(Map.Entry<?, ?> entry)
            {
                return entry.getValue();
            }
        };
    }

    @SuppressWarnings("unchecked")
    static <K extends @Nullable Object> Function<Map.Entry<K, ?>, K> keyFunction()
    {
        return (Function) EntryFunction.KEY;
    }

    @SuppressWarnings("unchecked")
    static <V extends @Nullable Object> Function<Map.Entry<?, V>, V> valueFunction()
    {
        return (Function) EntryFunction.VALUE;
    }

    @GwtCompatible(serializable = true)
    public static <K extends @Nullable Object, V extends @Nullable Object> Map.Entry<K, V> immutableEntry(
            @ParametricNullness K key, @ParametricNullness V value)
    {
        return new ImmutableEntry<>(key, value);
    }

    abstract static class IteratorBasedAbstractMap<
            K extends @Nullable Object, V extends @Nullable Object>
            extends AbstractMap<K, V>
    {
        @Override
        public abstract int size();

        abstract Iterator<Entry<K, V>> entryIterator();

        Spliterator<Entry<K, V>> entrySpliterator()
        {
            return Spliterators.spliterator(
                    entryIterator(), size(), Spliterator.SIZED | Spliterator.DISTINCT);
        }

        @Override
        public Set<Entry<K, V>> entrySet()
        {
            return new EntrySet<K, V>()
            {
                @Override
                Map<K, V> map()
                {
                    return IteratorBasedAbstractMap.this;
                }

                @Override
                public Iterator<Entry<K, V>> iterator()
                {
                    return entryIterator();
                }

                @Override
                public Spliterator<Entry<K, V>> spliterator()
                {
                    return entrySpliterator();
                }

                @Override
                public void forEach(Consumer<? super Entry<K, V>> action)
                {
                    forEachEntry(action);
                }
            };
        }

        void forEachEntry(Consumer<? super Entry<K, V>> action)
        {
            entryIterator().forEachRemaining(action);
        }

        @Override
        public void clear()
        {
            Iterators.clear(entryIterator());
        }
    }

    static class KeySet<K extends @Nullable Object, V extends @Nullable Object>
            extends Sets.ImprovedAbstractSet<K>
    {
        @Weak final Map<K, V> map;

        KeySet(Map<K, V> map)
        {
            this.map = requireNonNull(map);
        }

        Map<K, V> map()
        {
            return map;
        }

        @Override
        public Iterator<K> iterator()
        {
            return keyIterator(map().entrySet().iterator());
        }

        @Override
        public void forEach(Consumer<? super K> action)
        {
            requireNonNull(action);
            // avoids entry allocation for those maps that allocate entries on iteration
            map.forEach((k, v) -> action.accept(k));
        }

        @Override
        public int size()
        {
            return map().size();
        }

        @Override
        public boolean isEmpty()
        {
            return map().isEmpty();
        }

        @Override
        public boolean contains(@CheckForNull Object o)
        {
            return map().containsKey(o);
        }

        @Override
        public boolean remove(@CheckForNull Object o)
        {
            if (contains(o)) {
                map().remove(o);
                return true;
            }
            return false;
        }

        @Override
        public void clear()
        {
            map().clear();
        }
    }

    static <K extends @Nullable Object, V extends @Nullable Object> Iterator<K> keyIterator(
            Iterator<Map.Entry<K, V>> entryIterator)
    {
        return new TransformedIterator<Map.Entry<K, V>, K>(entryIterator)
        {
            @Override
            @ParametricNullness
            K transform(Map.Entry<K, V> entry)
            {
                return entry.getKey();
            }
        };
    }

    abstract static class EntrySet<K extends @Nullable Object, V extends @Nullable Object>
            extends Sets.ImprovedAbstractSet<Map.Entry<K, V>>
    {
        abstract Map<K, V> map();

        @Override
        public int size()
        {
            return map().size();
        }

        @Override
        public void clear()
        {
            map().clear();
        }

        @Override
        public boolean contains(@CheckForNull Object o)
        {
            if (o instanceof Map.Entry) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
                Object key = entry.getKey();
                V value = safeGet(map(), key);
                return Objects.equals(value, entry.getValue()) && (value != null || map().containsKey(key));
            }
            return false;
        }

        @Override
        public boolean isEmpty()
        {
            return map().isEmpty();
        }

        @Override
        public boolean remove(@CheckForNull Object o)
        {
            /*
             * `o instanceof Entry` is guaranteed by `contains`, but we check it here to satisfy our
             * nullness checker.
             */
            if (contains(o) && o instanceof Map.Entry) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
                return map().keySet().remove(entry.getKey());
            }
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c)
        {
            try {
                return super.removeAll(requireNonNull(c));
            }
            catch (UnsupportedOperationException e) {
                // if the iterators don't support remove
                return Sets.removeAllImpl(this, c.iterator());
            }
        }

        @Override
        public boolean retainAll(Collection<?> c)
        {
            try {
                return super.retainAll(requireNonNull(c));
            }
            catch (UnsupportedOperationException e) {
                // if the iterators don't support remove
                Set<@Nullable Object> keys = Sets.newHashSetWithExpectedSize(c.size());
                for (Object o : c) {
                    /*
                     * `o instanceof Entry` is guaranteed by `contains`, but we check it here to satisfy our
                     * nullness checker.
                     */
                    if (contains(o) && o instanceof Map.Entry) {
                        Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
                        keys.add(entry.getKey());
                    }
                }
                return map().keySet().retainAll(keys);
            }
        }
    }

    @CheckForNull
    static <V extends @Nullable Object> V safeGet(Map<?, V> map, @CheckForNull Object key)
    {
        requireNonNull(map);
        try {
            return map.get(key);
        }
        catch (ClassCastException | NullPointerException e) {
            return null;
        }
    }

    static int capacity(int expectedSize)
    {
        if (expectedSize < 3) {
            checkNonnegative(expectedSize, "expectedSize");
            return expectedSize + 1;
        }
        if (expectedSize < Ints.MAX_POWER_OF_TWO) {
            // This is the calculation used in JDK8 to resize when a putAll
            // happens; it seems to be the most conservative calculation we
            // can make.  0.75 is the default load factor.
            return (int) ((float) expectedSize / 0.75F + 1.0F);
        }
        return Integer.MAX_VALUE; // any large value
    }

    @CanIgnoreReturnValue
    static int checkNonnegative(int value, String name)
    {
        if (value < 0) {
            throw new IllegalArgumentException(name + " cannot be negative but was: " + value);
        }
        return value;
    }

    static class Values<K extends @Nullable Object, V extends @Nullable Object>
            extends AbstractCollection<V>
    {
        @Weak final Map<K, V> map;

        Values(Map<K, V> map)
        {
            this.map = requireNonNull(map);
        }

        final Map<K, V> map()
        {
            return map;
        }

        @Override
        public Iterator<V> iterator()
        {
            return valueIterator(map().entrySet().iterator());
        }

        @Override
        public void forEach(Consumer<? super V> action)
        {
            requireNonNull(action);
            // avoids allocation of entries for those maps that generate fresh entries on iteration
            map.forEach((k, v) -> action.accept(v));
        }

        @Override
        public boolean remove(@CheckForNull Object o)
        {
            try {
                return super.remove(o);
            }
            catch (UnsupportedOperationException e) {
                for (Map.Entry<K, V> entry : map().entrySet()) {
                    if (Objects.equals(o, entry.getValue())) {
                        map().remove(entry.getKey());
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public boolean removeAll(Collection<?> c)
        {
            try {
                return super.removeAll(requireNonNull(c));
            }
            catch (UnsupportedOperationException e) {
                Set<K> toRemove = new HashSet<>();
                for (Map.Entry<K, V> entry : map().entrySet()) {
                    if (c.contains(entry.getValue())) {
                        toRemove.add(entry.getKey());
                    }
                }
                return map().keySet().removeAll(toRemove);
            }
        }

        @Override
        public boolean retainAll(Collection<?> c)
        {
            try {
                return super.retainAll(requireNonNull(c));
            }
            catch (UnsupportedOperationException e) {
                Set<K> toRetain = new HashSet<>();
                for (Map.Entry<K, V> entry : map().entrySet()) {
                    if (c.contains(entry.getValue())) {
                        toRetain.add(entry.getKey());
                    }
                }
                return map().keySet().retainAll(toRetain);
            }
        }

        @Override
        public int size()
        {
            return map().size();
        }

        @Override
        public boolean isEmpty()
        {
            return map().isEmpty();
        }

        @Override
        public boolean contains(@CheckForNull Object o)
        {
            return map().containsValue(o);
        }

        @Override
        public void clear()
        {
            map().clear();
        }
    }

    static <K extends @Nullable Object, V extends @Nullable Object> Iterator<V> valueIterator(
            Iterator<Map.Entry<K, V>> entryIterator)
    {
        return new TransformedIterator<Map.Entry<K, V>, V>(entryIterator)
        {
            @Override
            @ParametricNullness
            V transform(Map.Entry<K, V> entry)
            {
                return entry.getValue();
            }
        };
    }
}
