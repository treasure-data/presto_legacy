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
package com.facebook.presto.util.list;

import com.facebook.presto.util.array.LongBigArray;

import java.util.AbstractList;

public class LongBigArrayList
        extends AbstractList<Long>
{
    private LongBigArray array = new LongBigArray();
    private int size;

    public LongBigArrayList()
    {
    }

    public LongBigArrayList(int capacity)
    {
        array.ensureCapacity(capacity);
    }

    @Override
    public int size()
    {
        return size;
    }

    public long sizeOf()
    {
        return array.sizeOf();
    }

    public long getLong(int index)
    {
        return array.get(index);
    }

    @Override
    public Long get(int index)
    {
        return getLong(index);
    }

    public long set(int index, long value)
    {
        array.ensureCapacity(index + 1);
        long old = array.get(index);
        array.set(index, value);

        return old;
    }

    @Override
    public Long set(int index, Long value)
    {
        long old = array.get(index);
        array.set(index, value);
        return old;
    }

    public boolean add(long value)
    {
        array.ensureCapacity(size + 1);
        array.set(size++, value);
        return true;
    }

    @Override
    public boolean add(Long value)
    {
        array.ensureCapacity(size + 1);
        array.set(size++, value);
        return true;
    }

    public void swap(int a, int b)
    {
        long temp = set(a, getLong(b));
        set(b, temp);
    }

    @Override
    public void clear()
    {
        array = new LongBigArray();
        size = 0;
    }

    public long[] toLongArray(long[] target)
    {
        if (target == null || target.length < size) {
            target = new long[size];
        }

        return array.toArray(target);
    }
}
