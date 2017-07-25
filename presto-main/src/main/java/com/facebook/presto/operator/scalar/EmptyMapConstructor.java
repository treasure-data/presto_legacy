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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;

public final class EmptyMapConstructor
{
    private final Block emptyMap;

    public EmptyMapConstructor(@TypeParameter("map(unknown,unknown)") Type mapType)
    {
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(new BlockBuilderStatus(), 1);
        mapBlockBuilder.beginBlockEntry();
        mapBlockBuilder.closeEntry();
        emptyMap = ((MapType) mapType).getObject(mapBlockBuilder.build(), 0);
    }

    @Description("Creates an empty map")
    @ScalarFunction
    @SqlType("map(unknown,unknown)")
    public Block map()
    {
        return emptyMap;
    }
}
