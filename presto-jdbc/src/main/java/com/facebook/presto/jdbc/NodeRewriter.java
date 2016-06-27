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
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
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
import com.facebook.presto.sql.tree.TransactionMode;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;

public class NodeRewriter<C>
{
    protected Node rewriteNode(Node node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return null;
    }

    protected Node rewriteStatement(Statement node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewritePrepare(Prepare node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteDeallocate(Deallocate node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteExecute(Execute node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteQuery(Query node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteExplain(Explain node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteShowTables(ShowTables node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteShowSchemas(ShowSchemas node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteShowCatalogs(ShowCatalogs node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteShowColumns(ShowColumns node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteShowPartitions(ShowPartitions node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteShowCreate(ShowCreate node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteShowFunctions(ShowFunctions node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteUse(Use node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteShowSession(ShowSession node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteSetSession(SetSession node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    public Node rewriteResetSession(ResetSession node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteExplainOption(ExplainOption node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteWith(With node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteApproximate(Approximate node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteWithQuery(WithQuery node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteSelect(Select node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteRelation(Relation node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteQueryBody(QueryBody node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteRelation(node, context, treeRewriter);
    }

    protected Node rewriteQuerySpecification(QuerySpecification node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteQueryBody(node, context, treeRewriter);
    }

    protected Node rewriteSetOperation(SetOperation node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteQueryBody(node, context, treeRewriter);
    }

    protected Node rewriteUnion(Union node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteSetOperation(node, context, treeRewriter);
    }

    protected Node rewriteIntersect(Intersect node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteSetOperation(node, context, treeRewriter);
    }

    protected Node rewriteExcept(Except node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteSetOperation(node, context, treeRewriter);
    }

    protected Node rewriteSelectItem(SelectItem node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteSingleColumn(SingleColumn node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteSelectItem(node, context, treeRewriter);
    }

    protected Node rewriteAllColumns(AllColumns node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteSelectItem(node, context, treeRewriter);
    }

    protected Node rewriteSortItem(SortItem node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteTable(Table node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteQueryBody(node, context, treeRewriter);
    }

    protected Node rewriteUnnest(Unnest node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteRelation(node, context, treeRewriter);
    }

    protected Node rewriteValues(Values node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteQueryBody(node, context, treeRewriter);
    }

    protected Node rewriteRow(Row node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteTableSubquery(TableSubquery node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteQueryBody(node, context, treeRewriter);
    }

    protected Node rewriteAliasedRelation(AliasedRelation node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteRelation(node, context, treeRewriter);
    }

    protected Node rewriteSampledRelation(SampledRelation node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteRelation(node, context, treeRewriter);
    }

    protected Node rewriteJoin(Join node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteRelation(node, context, treeRewriter);
    }

    protected Node rewriteWindow(Window node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteWindowFrame(WindowFrame node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteFrameBound(FrameBound node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteCallArgument(CallArgument node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteTableElement(TableElement node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteCreateTable(CreateTable node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteCreateTableAsSelect(CreateTableAsSelect node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteDropTable(DropTable node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteRenameTable(RenameTable node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteRenameColumn(RenameColumn node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteAddColumn(AddColumn node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteCreateView(CreateView node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteDropView(DropView node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteInsert(Insert node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteCall(Call node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteDelete(Delete node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteStartTransaction(StartTransaction node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteGrant(Grant node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteRevoke(Revoke node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteTransactionMode(TransactionMode node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteIsolationLevel(Isolation node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteTransactionMode(node, context, treeRewriter);
    }

    protected Node rewriteTransactionAccessMode(TransactionAccessMode node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteTransactionMode(node, context, treeRewriter);
    }

    protected Node rewriteCommit(Commit node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteRollback(Rollback node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteStatement(node, context, treeRewriter);
    }

    protected Node rewriteGroupBy(GroupBy node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteGroupingElement(GroupingElement node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteNode(node, context, treeRewriter);
    }

    protected Node rewriteCube(Cube node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteGroupingElement(node, context, treeRewriter);
    }

    protected Node rewriteGroupingSets(GroupingSets node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteGroupingElement(node, context, treeRewriter);
    }

    protected Node rewriteRollup(Rollup node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteGroupingElement(node, context, treeRewriter);
    }

    protected Node rewriteSimpleGroupBy(SimpleGroupBy node, C context, NodeTreeRewriter<C> treeRewriter)
    {
        return rewriteGroupingElement(node, context, treeRewriter);
    }
}
