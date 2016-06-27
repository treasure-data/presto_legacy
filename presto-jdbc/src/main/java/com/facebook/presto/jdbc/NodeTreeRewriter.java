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

import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Approximate;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Isolation;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TransactionAccessMode;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public final class NodeTreeRewriter<C>
{
    private final NodeRewriter<C> rewriter;
    private final ExpressionTreeRewriter<C> expressionTreeRewriter;
    private final AstVisitor<Node, NodeTreeRewriter.Context<C>> visitor;

    public static <C, T extends Node> T rewriteWith(NodeRewriter<C> rewriter, ExpressionRewriter<C> expressionRewriter,
            T node)
    {
        return new NodeTreeRewriter<>(rewriter, expressionRewriter).rewrite(node, null);
    }

    public static <C, T extends Node> T rewriteWith(NodeRewriter<C> rewriter, ExpressionRewriter<C> expressionRewriter,
            T node, C context)
    {
        return new NodeTreeRewriter<>(rewriter, expressionRewriter).rewrite(node, context);
    }

    public NodeTreeRewriter(NodeRewriter<C> rewriter, ExpressionRewriter<C> expressionRewriter)
    {
        this.rewriter = rewriter;
        this.expressionTreeRewriter = new ExpressionTreeRewriter<C>(expressionRewriter);
        this.visitor = new RewritingVisitor();
    }

    @SuppressWarnings("unchecked")
    public <T extends Node> T rewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the expression rewriter for
     * the provided node.
     */
    @SuppressWarnings("unchecked")
    public <T extends Node> T defaultRewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, true));
    }

    private class RewritingVisitor extends AstVisitor<Node, NodeTreeRewriter.Context<C>>
    {
        @Override
        protected Node visitNode(Node node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteNode(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            throw new UnsupportedOperationException(
                    "not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass().getName());
        }

        @Override
        protected Node visitPrepare(Prepare node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewritePrepare(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitDeallocate(Deallocate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteDeallocate(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitExecute(Execute node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteExecute(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitQuery(Query node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteQuery(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Optional<With> with = node.getWith().map(x -> rewrite(x, context.get()));
            QueryBody queryBody = rewrite(node.getQueryBody(), context.get());
            ImmutableList<SortItem> orderBy = rewriteOrderBy(node.getOrderBy(), context.get());
            Optional<Approximate> approximate = node.getApproximate().map(x -> rewrite(x, context.get()));
            return new Query(with, queryBody, orderBy, node.getLimit(), approximate);
        }

        @Override
        protected Node visitExplain(Explain node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteExplain(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Statement stmt = rewrite(node.getStatement(), context.get());
            ImmutableList.Builder<ExplainOption> explainOption = ImmutableList.builder();
            for (ExplainOption e : node.getOptions()) {
                explainOption.add(rewrite(e, context.get()));
            }
            return new Explain(stmt, node.isAnalyze(), explainOption.build());
        }

        @Override
        protected Node visitShowTables(ShowTables node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteShowTables(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitShowSchemas(ShowSchemas node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteShowSchemas(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitShowCatalogs(ShowCatalogs node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteShowCatalogs(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitShowColumns(ShowColumns node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteShowColumns(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitShowPartitions(ShowPartitions node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteShowPartitions(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Optional<Expression> where = rewriteExpression(node.getWhere(), context.get());
            ImmutableList<SortItem> orderBy = rewriteOrderBy(node.getOrderBy(), context.get());
            return new ShowPartitions(node.getTable(), where, orderBy, node.getLimit());
        }

        @Override
        protected Node visitShowCreate(ShowCreate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteShowCreate(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitShowFunctions(ShowFunctions node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteShowFunctions(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitUse(Use node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteUse(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitShowSession(ShowSession node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteShowSession(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitSetSession(SetSession node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteSetSession(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = expressionTreeRewriter.rewrite(node.getValue(), context.get());
            return new SetSession(node.getName(), value);
        }

        // TODO: this must be protected
        @Override
        public Node visitResetSession(ResetSession node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteResetSession(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitWith(With node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteWith(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<WithQuery> builder = ImmutableList.builder();
            for (WithQuery e : node.getQueries()) {
                builder.add(rewrite(e, context.get()));
            }
            return new With(node.isRecursive(), builder.build());
        }

        @Override
        protected Node visitApproximate(Approximate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteApproximate(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitWithQuery(WithQuery node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteWithQuery(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Query query = rewrite(node.getQuery(), context.get());
            return new WithQuery(node.getName(), query, node.getColumnNames());
        }

        @Override
        protected Node visitSelect(Select node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteSelect(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
            for (SelectItem e : node.getSelectItems()) {
                selectItems.add(rewrite(e, context.get()));
            }
            return new Select(node.isDistinct(), selectItems.build());
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteQuerySpecification(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Select select = rewrite(node.getSelect(), context.get());
            Optional<Relation> from = node.getFrom().map(x -> rewrite(x, context.get()));
            Optional<Expression> where = rewriteExpression(node.getWhere(), context.get());
            Optional<GroupBy> groupBy = node.getGroupBy().map(x -> rewrite(x, context.get()));
            Optional<Expression> having = rewriteExpression(node.getHaving(), context.get());
            List<SortItem> orderBy = rewriteOrderBy(node.getOrderBy(), context.get());
            return new QuerySpecification(select, from, where, groupBy, having, orderBy, node.getLimit());
        }

        @Override
        protected Node visitUnion(Union node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteUnion(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<Relation> relations = ImmutableList.builder();
            for (Relation e : node.getRelations()) {
                relations.add(rewrite(e, context.get()));
            }
            return new Union(relations.build(), node.isDistinct());
        }

        @Override
        protected Node visitIntersect(Intersect node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteIntersect(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitExcept(Except node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteExcept(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitSingleColumn(SingleColumn node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteSingleColumn(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewriteExpression(node.getExpression(), context.get());
            return new SingleColumn(expression, node.getAlias());
        }

        @Override
        protected Node visitAllColumns(AllColumns node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteAllColumns(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitSortItem(SortItem node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteSortItem(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression sortKey = rewriteExpression(node.getSortKey(), context.get());
            return new SortItem(sortKey, node.getOrdering(), node.getNullOrdering());
        }

        @Override
        protected Node visitTable(Table node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteTable(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitUnnest(Unnest node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteUnnest(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitValues(Values node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteValues(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList<Expression> rows = rewriteExpressions(node.getRows(), context);
            return new Values(rows);
        }

        @Override
        protected Node visitTableSubquery(TableSubquery node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteTableSubquery(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitAliasedRelation(AliasedRelation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteAliasedRelation(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Relation relation = rewrite(node.getRelation(), context.get());
            return new AliasedRelation(relation, node.getAlias(), node.getColumnNames());
        }

        @Override
        protected Node visitSampledRelation(SampledRelation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteSampledRelation(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitJoin(Join node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteJoin(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Relation left = rewrite(node.getLeft(), context.get());
            Relation right = rewrite(node.getRight(), context.get());
            // TODO: JoinOn has Expression inside so it should call expressionTreeRewriter.
            return new Join(node.getType(), left, right, node.getCriteria());
        }

        @Override
        protected Node visitWindow(Window node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteWindow(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitWindowFrame(WindowFrame node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteWindowFrame(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitFrameBound(FrameBound node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteFrameBound(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitCallArgument(CallArgument node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteCallArgument(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitTableElement(TableElement node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteTableElement(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitCreateTable(CreateTable node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteCreateTable(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitCreateTableAsSelect(CreateTableAsSelect node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteCreateTableAsSelect(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitDropTable(DropTable node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteDropTable(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitRenameTable(RenameTable node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteRenameTable(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitRenameColumn(RenameColumn node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteRenameColumn(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitAddColumn(AddColumn node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteAddColumn(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitCreateView(CreateView node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteCreateView(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitDropView(DropView node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteDropView(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitInsert(Insert node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteInsert(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Query query = rewrite(node.getQuery(), context.get());
            return new Insert(node.getTarget(), node.getColumns(), query);
        }

        @Override
        protected Node visitCall(Call node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteCall(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitDelete(Delete node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteDelete(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitStartTransaction(StartTransaction node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteStartTransaction(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitGrant(Grant node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteGrant(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            // TODO
            return visitNode(node, context);
        }

        @Override
        protected Node visitRevoke(Revoke node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteRevoke(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitIsolationLevel(Isolation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteIsolationLevel(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitTransactionAccessMode(TransactionAccessMode node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteTransactionAccessMode(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitCommit(Commit node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteCommit(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitRollback(Rollback node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteRollback(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitGroupBy(GroupBy node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteGroupBy(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<GroupingElement> groupingElements = ImmutableList.builder();
            for (GroupingElement e : node.getGroupingElements()) {
                groupingElements.add(rewrite(e, context.get()));
            }
            return new GroupBy(node.isDistinct(), groupingElements.build());
        }

        @Override
        protected Node visitCube(Cube node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteCube(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitGroupingSets(GroupingSets node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteGroupingSets(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitRollup(Rollup node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteRollup(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return node;
        }

        @Override
        protected Node visitSimpleGroupBy(SimpleGroupBy node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Node result = rewriter.rewriteSimpleGroupBy(node, context.get(), NodeTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList<Expression> simpleGroupByExpressions = rewriteExpressions(node.getColumnExpressions(),
                    context);
            return new SimpleGroupBy(simpleGroupByExpressions);
        }

        ///

        private ImmutableList<SortItem> rewriteOrderBy(List<SortItem> sortItems, C context)
        {
            ImmutableList.Builder<SortItem> orderBy = ImmutableList.builder();
            for (SortItem e : sortItems) {
                orderBy.add(rewrite(e, context));
            }
            return orderBy.build();
        }

        private ImmutableList<Expression> rewriteExpressions(List<Expression> expressions, Context<C> context)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (Expression e : expressions) {
                builder.add(rewriteExpression(e, context.get()));
            }
            return builder.build();
        }

        private Expression rewriteExpression(Expression node, C context)
        {
            return expressionTreeRewriter.rewrite(node, context);
        }

        private Optional<Expression> rewriteExpression(Optional<Expression> node, C context)
        {
            return node.map(x -> rewriteExpression(x, context));
        }
    }

    public static class Context<C>
    {
        private final boolean defaultRewrite;
        private final C context;

        private Context(C context, boolean defaultRewrite)
        {
            this.context = context;
            this.defaultRewrite = defaultRewrite;
        }

        public C get()
        {
            return context;
        }

        public boolean isDefaultRewrite()
        {
            return defaultRewrite;
        }
    }
}
