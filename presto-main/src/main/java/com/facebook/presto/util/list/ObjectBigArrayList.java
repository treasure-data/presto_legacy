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

import com.facebook.presto.util.array.ObjectBigArray;

import java.util.AbstractList;

public class ObjectBigArrayList<T>
        extends AbstractList<T>
{
    private ObjectBigArray<T> array = new ObjectBigArray();
    private int size;

    @Override
    public int size()
    {
        return size;
    }

    public long sizeOf()
    {
        return array.sizeOf();
    }

    @Override
    public T get(int index)
    {
        return array.get(index);
    }

    @Override
    public T set(int index, T value)
    {
        array.ensureCapacity(index + 1);
        T old = array.get(index);
        array.set(index, value);

        return old;
    }

    @Override
    public boolean add(T value)
    {
        array.ensureCapacity(size + 1);
        array.set(size++, value);
        return true;
    }

    public void swap(int a, int b)
    {
        T temp = set(a, get(b));
        set(b, temp);
    }

    @Override
    public void clear()
    {
        array = new ObjectBigArray();
        size = 0;
    }
}
