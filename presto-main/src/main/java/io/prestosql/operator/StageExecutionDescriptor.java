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
package io.prestosql.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StageExecutionDescriptor
{
    private final Set<PlanNodeId> groupedExecutionScanNodes;

    private StageExecutionDescriptor(Set<PlanNodeId> groupedExecutionScanNodes)
    {
        this.groupedExecutionScanNodes = groupedExecutionScanNodes;
    }

    public static StageExecutionDescriptor ungroupedExecution()
    {
        return new StageExecutionDescriptor(ImmutableSet.of());
    }

    public static StageExecutionDescriptor groupedExecution(List<PlanNodeId> capableScanNodes)
    {
        requireNonNull(capableScanNodes, "capableScanNodes is null");
        checkArgument(!capableScanNodes.isEmpty());
        return new StageExecutionDescriptor(ImmutableSet.copyOf(capableScanNodes));
    }

    public boolean isStageGroupedExecution()
    {
        return !groupedExecutionScanNodes.isEmpty();
    }

    public boolean isScanGroupedExecution(PlanNodeId scanNodeId)
    {
        return groupedExecutionScanNodes.contains(scanNodeId);
    }

    @JsonCreator
    public static StageExecutionDescriptor jsonCreator(
            @JsonProperty("groupedExecutionScanNodes") Set<PlanNodeId> groupedExecutionCapableScanNodes)
    {
        return new StageExecutionDescriptor(ImmutableSet.copyOf(requireNonNull(groupedExecutionCapableScanNodes, "groupedExecutionScanNodes is null")));
    }

    @JsonProperty("groupedExecutionScanNodes")
    public Set<PlanNodeId> getJsonSerializableGroupedExecutionScanNodes()
    {
        return groupedExecutionScanNodes;
    }
}
