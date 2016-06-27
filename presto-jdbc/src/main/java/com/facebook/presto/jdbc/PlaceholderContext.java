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
package com.facebook.presto.jdbc;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlaceholderContext
{
    public static PlaceholderContext scan(SqlParser parser, String sql)
    {
        // 1. find a marker; this cannot be String to prevent conflicting with '?' in String
        // literal.
        long marker = findMarker(sql);
        // 2. replace all "?" with the marker so that SqlParser can parse.
        sql = sql.replace("?", String.valueOf(marker));
        // 3. parse sql
        Statement statement = parser.createStatement(sql);
        // 4. check all marker occurrences and remember the position
        PlaceholderContext ctx = new PlaceholderContext(sql, statement, marker);
        new MarkerScanVisitor().process(statement, ctx);
        ctx.onFinished();
        return ctx;
    }

    public Statement rewrite(List<Literal> parameters)
    {
        // 5. replace marker with values
        return new PreparedStatementNodeTreeRewriter(this).rewrite(statement, parameters);
    }

    private final String sql;
    private final Statement statement;
    private final long marker;
    private final List<NodeLocation> locations = new ArrayList<>();
    private final Map<NodeLocation, Integer> locationsMap = new HashMap<>();
    private NodeTreeRewriter<PlaceholderContext> nodeTreeRewriter = null;

    public PlaceholderContext(String sql, Statement statement, long marker)
    {
        this.sql = sql;
        this.statement = statement;
        this.marker = marker;
    }

    public String getSql()
    {
        return sql;
    }

    void setNodeTreeRewriter(NodeTreeRewriter<PlaceholderContext> nodeTreeRewriter)
    {
        this.nodeTreeRewriter = nodeTreeRewriter;
    }

    Integer getMarkerIndex(NodeLocation location)
    {
        return locationsMap.get(location);
    }

    StringLiteral getRecoveredStringLiteral(StringLiteral replaced)
    {
        return new StringLiteral(replaced.getValue().replace(String.valueOf(marker), "?"));
    }

    Node rewriteNode(Node node)
    {
        if (nodeTreeRewriter != null) {
            return nodeTreeRewriter.rewrite(node, this);
        }
        return node;
    }

    void checkMarker(LongLiteral node)
    {
        if (node.getValue() == marker) {
            node.getLocation().ifPresent(x -> locations.add(x));
        }
    }

    void onFinished()
    {
        List<NodeLocation> result = new ArrayList<>(locations);
        result.sort((a, b) -> (a.getLineNumber() == b.getLineNumber())
                ? Integer.compare(a.getColumnNumber(), b.getColumnNumber())
                : Integer.compare(a.getLineNumber(), b.getLineNumber()));
        for (int idx = 0; idx < result.size(); ++idx) {
            locationsMap.put(result.get(idx), Integer.valueOf(idx));
        }
    }

    // find a marker which does not exist in sql.
    private static long findMarker(String sql)
    {
        long idx = 100000000;
        while (true) {
            if (sql.indexOf(String.valueOf(idx)) == -1) {
                return idx;
            }
            ++idx;
        }
    }

    private static class MarkerScanVisitor extends DefaultTraversalVisitor<Void, PlaceholderContext>
    {
        @Override
        protected Void visitLongLiteral(LongLiteral node, PlaceholderContext context)
        {
            context.checkMarker(node);
            return visitLiteral(node, context);
        }
    }

    public class PreparedStatementNodeTreeRewriter
    {
        private final PlaceholderContext context;

        public PreparedStatementNodeTreeRewriter(PlaceholderContext context)
        {
            this.context = context;
        }

        public <T extends Node> T rewrite(T node, List<Literal> parameters)
        {
            NodeTreeRewriter<PlaceholderContext> nodeTreeRewriter = new NodeTreeRewriter<>(
                    new NodeRewriter<PlaceholderContext>(),
                    new PreparedStatementPlaceholderExpressionRewriter(parameters));
            context.setNodeTreeRewriter(nodeTreeRewriter);
            return nodeTreeRewriter.rewrite(node, context);
        }

        private class PreparedStatementPlaceholderExpressionRewriter extends ExpressionRewriter<PlaceholderContext>
        {
            private final List<Literal> parameters;

            public PreparedStatementPlaceholderExpressionRewriter(List<Literal> parameters)
            {
                this.parameters = parameters;
            }

            @Override
            public Expression rewriteLiteral(Literal node, PlaceholderContext context,
                    ExpressionTreeRewriter<PlaceholderContext> treeRewriter)
            {
                if (node instanceof LongLiteral) {
                    Integer replace = context.getMarkerIndex(node.getLocation().get());
                    if (replace != null) {
                        return parameters.get(replace.intValue());
                    }
                }
                else if (node instanceof StringLiteral) {
                    return context.getRecoveredStringLiteral((StringLiteral) node);
                }
                return super.rewriteLiteral(node, context, treeRewriter);
            }

            @Override
            public Expression rewriteSubqueryExpression(SubqueryExpression node, PlaceholderContext context,
                    ExpressionTreeRewriter<PlaceholderContext> treeRewriter)
            {
                return new SubqueryExpression((Query) context.rewriteNode(node.getQuery()));
            }
        }
    }
}
