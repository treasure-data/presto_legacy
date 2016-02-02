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
package com.facebook.presto.server;

import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.StageStats;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

@Path("/")
public class QueryPlanResource
{
    private final QueryManager manager;

    @Inject
    public QueryPlanResource(QueryManager manager)
    {
        requireNonNull(manager, "manager is null");
        this.manager = manager;
    }

    @GET
    @Path("/v1/plan")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQueryPlan()
    {
        ImmutableList.Builder<QuerySummary> stageSummary = ImmutableList.builder();
        for (QueryInfo queryInfo : manager.getAllQueryInfo()) {
            stageSummary.add(new QuerySummary(queryInfo));
        }
        return Response.ok(stageSummary.build()).build();
    }

    @GET
    @Path("/v1/plan/{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQueryPlan(@PathParam("queryId") String queryId)
    {
        QuerySummary summary;
        try {
            summary = getQuerySummary(queryId);
        }
        catch (NoSuchElementException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok(summary).build();
    }

    private QuerySummary getQuerySummary(String queryId)
    {
        return new QuerySummary(manager.getQueryInfo(QueryId.valueOf(queryId)));
    }

    public static class QuerySummary
            extends BasicQueryInfo
    {
        private final List<StageSummary> stages;

        public QuerySummary(QueryInfo queryInfo)
        {
            super(queryInfo);
            this.stages = flattenStages(queryInfo.getOutputStage(), 0, 0);
        }

        @JsonProperty
        public List<StageSummary> getStages()
        {
            return stages;
        }
    }

    private static List<StageSummary> flattenStages(StageInfo stage, int indentLevel, int siblingLevel)
    {
        if (stage == null) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<StageSummary> stages = ImmutableList.builder();
        stages.add(new StageSummary(stage, indentLevel, siblingLevel));
        int currentSiblingLevel = siblingLevel;
        for (StageInfo child : stage.getSubStages()) {
            stages.addAll(flattenStages(child, indentLevel + 1, currentSiblingLevel));
            currentSiblingLevel++;
        }
        return stages.build();
    }

    public static class StageSummary
    {
        private final StageId stageId;
        private final StageState state;
        private final StageStats stats;
        private final int indentLevel;
        private final int siblingIndex;
        private final PlanFragment planFragment;
        private final PlanNodeSummary plan;

        public StageSummary(StageInfo stage, int indentLevel, int siblingIndex)
        {
            this.stageId = stage.getStageId();
            this.state = stage.getState();
            this.stats = stage.getStageStats();
            this.indentLevel = indentLevel;
            this.siblingIndex = siblingIndex;
            this.planFragment = stage.getPlan();
            this.plan = new PlanNodeSummary(stage.getPlan().getRoot());
        }

        @JsonProperty
        public StageId getStageId()
        {
            return stageId;
        }

        @JsonProperty
        public StageState getState()
        {
            return state;
        }

        @JsonProperty
        @JsonIgnoreProperties({"getSplitDistribution", "scheduleTaskDistribution", "addSplitDistribution"})
        public StageStats getStats()
        {
            return stats;
        }

        @JsonProperty
        public int getIndentLevel()
        {
            return indentLevel;
        }

        @JsonProperty
        public int getSiblingIndex()
        {
            return siblingIndex;
        }

        @JsonProperty
        @JsonIgnoreProperties({"root"})
        public PlanFragment getPlanFragement()
        {
            return planFragment;
        }

        @JsonProperty
        public PlanNodeSummary getPlan()
        {
            return plan;
        }
    }

    public static class PlanNodeSummary
    {
        private final PlanNode planNode;
        private final List<PlanNodeSummary> sources;
        private final List<PlanFragmentId> remoteSources;

        private static List<PlanNodeSummary> collectSources(PlanNode node)
        {
            ImmutableList.Builder<PlanNodeSummary> sources = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                sources.add(new PlanNodeSummary(source));
            }
            return sources.build();
        }

        private static List<PlanFragmentId> collectRemoteSources(PlanNode node)
        {
            if (node instanceof RemoteSourceNode) {
                return ((RemoteSourceNode) node).getSourceFragmentIds();
            }
            else {
                return ImmutableList.of();
            }
        }

        public PlanNodeSummary(PlanNode planNode)
        {
            this.planNode = planNode;
            this.sources = collectSources(planNode);
            this.remoteSources = collectRemoteSources(planNode);
        }

        @JsonProperty
        @JsonIgnoreProperties({"source", "sources", "remoteSources", "filteringSource", "probeSource", "filterSource", "left", "right"})
        public PlanNode getPlanNode()
        {
            return planNode;
        }

        @JsonProperty
        public List<PlanNodeSummary> getSources()
        {
            return sources;
        }

        @JsonProperty
        public List<PlanFragmentId> getRemoteSources()
        {
            return remoteSources;
        }
    }
}
