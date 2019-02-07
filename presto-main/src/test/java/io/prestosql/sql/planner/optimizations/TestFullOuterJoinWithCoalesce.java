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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;

public class TestFullOuterJoinWithCoalesce
        extends BasePlanTest
{
    @Test
    public void testFullOuterJoinWithCoalesce()
    {
        assertDistributedPlan("SELECT coalesce(ts.a, r.a) " +
                "FROM (" +
                "   SELECT coalesce(t.a, s.a) AS a " +
                "   FROM (VALUES (1), (2), (3)) t(a) " +
                "   FULL OUTER JOIN (VALUES (1), (4)) s(a)" +
                "   ON t.a = s.a) ts " +
                "FULL OUTER JOIN (VALUES (2), (5)) r(a) on ts.a = r.a",
                anyTree(
                        project(
                                ImmutableMap.of("expr", expression("coalesce(ts, r)")),
                                join(
                                        FULL,
                                        ImmutableList.of(equiJoinClause("ts", "r")),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("ts", expression("coalesce(t, s)")),
                                                        join(
                                                                FULL,
                                                                ImmutableList.of(equiJoinClause("t", "s")),
                                                                exchange(REMOTE, REPARTITION, anyTree(values(ImmutableList.of("t")))),
                                                                exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("s"))))))),
                                        exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("r"))))))));
    }
}
