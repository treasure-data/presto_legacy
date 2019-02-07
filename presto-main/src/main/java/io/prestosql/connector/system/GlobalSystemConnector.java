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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.transaction.InternalConnector;
import io.prestosql.transaction.TransactionId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GlobalSystemConnector
        implements InternalConnector
{
    public static final String NAME = "system";

    private final String connectorId;
    private final Set<SystemTable> systemTables;
    private final Set<Procedure> procedures;

    public GlobalSystemConnector(String connectorId, Set<SystemTable> systemTables, Set<Procedure> procedures)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.systemTables = ImmutableSet.copyOf(requireNonNull(systemTables, "systemTables is null"));
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new GlobalSystemTransactionHandle(connectorId, transactionId);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return new ConnectorMetadata()
        {
            @Override
            public List<String> listSchemaNames(ConnectorSession session)
            {
                return ImmutableList.of();
            }

            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
            {
                return null;
            }

            @Override
            public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
            {
                return ImmutableList.of();
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
            {
                return ImmutableMap.of();
            }
        };
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return (transactionHandle, session, layout, splitSchedulingStrategy) -> {
            throw new UnsupportedOperationException();
        };
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return (transactionHandle, session, split, columns) -> {
            throw new UnsupportedOperationException();
        };
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }
}
