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
package io.trino.plugin.paimon.catalog.hms;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveType;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.paimon.DatabaseLocationNotFound;
import io.trino.plugin.paimon.TableLocationNotFound;
import io.trino.plugin.paimon.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveTypeUtils;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.META_TABLE_STORAGE;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_COMMIT_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_FILESYSTEM_ERROR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.paimon.hive.HiveCatalog.COMMENT_PROP;
import static org.apache.paimon.hive.HiveCatalog.PAIMON_TABLE_TYPE_VALUE;
import static org.apache.paimon.hive.HiveCatalog.TABLE_TYPE_PROP;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;

public class TrinoHiveCatalog
        extends AbstractCatalog
        implements TrinoCatalog
{
    private static final Logger LOG = Logger.get(TrinoHiveCatalog.class);
    private static final Options EMPTY = new Options();

    public static final String DB_LOCATION_PROP = "location";
    private static final String INPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonInputFormat";
    private static final String OUTPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonOutputFormat";
    private static final String SERDE_CLASS_NAME = "org.apache.paimon.hive.PaimonSerDe";
    private static final StorageFormat PAIMON_METASTORE_STORAGE_FORMAT = StorageFormat.create(
            SERDE_CLASS_NAME,
            INPUT_FORMAT_CLASS_NAME,
            OUTPUT_FORMAT_CLASS_NAME);
    private static final String STORAGE_HANDLER_CLASS_NAME =
            "org.apache.paimon.hive.PaimonStorageHandler";
    private static final String HIVE_PREFIX = "hive.";

    private final CachingHiveMetastore metastore;
    private final ThriftMetastore thriftMetastore;
    private final ConnectorSession session;
    private final boolean catalogLockEnabled;
    private final boolean isUsingSystemSecurity;

    public TrinoHiveCatalog(
            CachingHiveMetastore metastore,
            ThriftMetastore thriftMetastore,
            ConnectorSession session,
            FileIO fileIO,
            boolean catalogLockEnabled,
            boolean isUsingSystemSecurity)
    {
        super(requireNonNull(fileIO, "fileIO is null"), EMPTY);
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.thriftMetastore = requireNonNull(thriftMetastore, "thriftMetastore is null");
        this.session = requireNonNull(session, "session is null");
        this.catalogLockEnabled = catalogLockEnabled;
        this.isUsingSystemSecurity = isUsingSystemSecurity;
    }

    @Override
    public String warehouse()
    {
        throw new UnsupportedOperationException("Not support to get warehouse path in hive catalog");
    }

    @Override
    public boolean lockEnabled()
    {
        return catalogLockEnabled;
    }

    @Override
    public Optional<CatalogLockFactory> lockFactory()
    {
        if (!lockEnabled()) {
            return Optional.empty();
        }

        return Optional.of(new TrinoHiveCatalogLockFactory(thriftMetastore, session));
    }

    @Override
    public Optional<CatalogLockContext> lockContext()
    {
        return Optional.empty();
    }

    @Override
    public Optional<MetastoreClient.Factory> metastoreClientFactory(Identifier identifier)
    {
        try {
            return Optional.of(new TrinoHiveMetastoreClient.Factory(metastore, identifier, getDataTableSchema(identifier)));
        }
        catch (TableNotExistException e) {
            throw new TableNotFoundException(new SchemaTableName(identifier.getDatabaseName(), identifier.getObjectName()));
        }
    }

    @Override
    public List<String> listDatabases()
    {
        return metastore.getAllDatabases();
    }

    @Override
    public Path getDataTableLocation(Identifier identifier)
    {
        String databaseName = identifier.getDatabaseName();
        String tableName = identifier.getObjectName();
        Optional<Table> table = metastore.getTable(databaseName, tableName);
        if (table.isPresent()) {
            String location = table.get().getStorage().getLocation();
            if (location != null) {
                return new Path(location);
            }
            throw new TableLocationNotFound(new SchemaTableName(databaseName, tableName));
        }
        else {
            Optional<Database> database = metastore.getDatabase(databaseName);
            if (database.isEmpty()) {
                throw new SchemaNotFoundException(databaseName);
            }
            // If the table does not exist,
            // we should use the database path to generate the table path.
            Optional<String> dbLocation = database.get().getLocation();
            if (dbLocation.isPresent()) {
                return new Path(dbLocation.get(), tableName);
            }
            throw new DatabaseLocationNotFound(databaseName);
        }
    }

    @Override
    protected Map<String, String> loadDatabasePropertiesImpl(String name)
    {
        Optional<Database> database = metastore.getDatabase(name);
        if (database.isEmpty()) {
            throw new SchemaNotFoundException(name);
        }
        return convertToProperties(database.get());
    }

    @Override
    public boolean databaseExistsImpl(String databaseName)
    {
        return metastore.getDatabase(databaseName).isPresent();
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties)
    {
        Database.Builder builder = Database.builder();
        builder.setDatabaseName(name);
        Map<String, String> parameter = new HashMap<>();
        properties.forEach(
                (key, value) -> {
                    if (key.equals(COMMENT_PROP)) {
                        builder.setComment(Optional.of(value));
                    }
                    else if (key.equals(DB_LOCATION_PROP)) {
                        builder.setLocation(Optional.of(value));
                    }
                    else if (value != null) {
                        parameter.put(key, value);
                    }
                });
        builder.setParameters(parameter);
        builder.setOwnerType(Optional.empty());
        builder.setOwnerName(Optional.empty());
        metastore.createDatabase(builder.build());
    }

    @Override
    protected void dropDatabaseImpl(String name)
    {
        metastore.dropDatabase(name, true);
    }

    @Override
    protected List<String> listTablesImpl(String databaseName)
    {
        return metastore.getTables(databaseName).stream().map(tableInfo -> tableInfo.tableName().getTableName()).filter(tableName -> {
            Identifier identifier = new Identifier(databaseName, tableName);
            return tableExists(identifier);
        }).collect(toList());
    }

    @Override
    public boolean tableExists(Identifier identifier)
    {
        if (isSystemTable(identifier)) {
            return super.tableExists(identifier);
        }
        Optional<Table> table = metastore.getTable(identifier.getDatabaseName(), identifier.getObjectName());
        return table.filter(TrinoHiveCatalog::isPaimonTable).isPresent();
    }

    @Override
    protected void dropTableImpl(Identifier identifier)
    {
        Optional<Table> table = metastore.getTable(identifier.getDatabaseName(), identifier.getObjectName());
        if (table.isEmpty()) {
            throw new TableNotFoundException(new SchemaTableName(identifier.getDatabaseName(), identifier.getObjectName()));
        }
        metastore.dropTable(identifier.getDatabaseName(), identifier.getObjectName(), true);

        Path location = new Path(table.get().getStorage().getLocation());
        try {
            if (fileIO.exists(location)) {
                fileIO.deleteDirectoryQuietly(location);
            }
        }
        catch (Exception ex) {
            LOG.error(ex, "Delete directory[%s] fail for table %s", location, identifier);
        }
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException
    {
        TableSchema tableSchema = getDataTableSchema(identifier);
        if (!tableSchema.partitionKeys().isEmpty()
                && new CoreOptions(tableSchema.options()).partitionedTableInMetastore()) {
            metastore.dropPartition(identifier.getDatabaseName(), identifier.getObjectName(), partitionSpec.values().stream().toList(), false);
        }
        super.dropPartition(identifier, partitionSpec);
    }

    @Override
    protected void createTableImpl(Identifier identifier, Schema schema)
    {
        TableSchema newSchema;
        Path location = getDataTableLocation(identifier);
        checkTableEmpty(identifier, location);
        try {
            newSchema = schemaManager(identifier, location).createTable(schema, false);
            Table.Builder builder = Table.builder()
                    .setDatabaseName(identifier.getDatabaseName())
                    .setTableName(identifier.getObjectName())
                    .setOwner(Optional.empty())
                    // Table needs to be EXTERNAL, otherwise table rename in HMS would rename table directory and break table contents.
                    .setTableType(EXTERNAL_TABLE.name())
                    .withStorage(storage -> storage.setStorageFormat(PAIMON_METASTORE_STORAGE_FORMAT))
                    // This is a must-have property for the EXTERNAL_TABLE table type
                    .setParameter("EXTERNAL", "TRUE")
                    .setParameter(TABLE_TYPE_PROP, PAIMON_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                    .setParameter(META_TABLE_STORAGE, STORAGE_HANDLER_CLASS_NAME);

            updateHmsTable(builder, newSchema, location);

            Table table = builder.build();
            metastore.createTable(table, NO_PRIVILEGES);
        }
        catch (Exception e) {
            // clean up metadata files corresponding to the current transaction
            try {
                fileIO.deleteDirectoryQuietly(location);
            }
            catch (Exception ee) {
                LOG.error(ee, "Delete directory[%s] fail for table %s", location, identifier);
            }
            throw new TrinoException(PAIMON_COMMIT_ERROR, format("Failed to commit table %s.", identifier.getFullName()), e);
        }
    }

    @Override
    public void renameTableImpl(Identifier fromTable, Identifier toTable)
    {
        metastore.renameTable(fromTable.getDatabaseName(), fromTable.getObjectName(), toTable.getDatabaseName(), toTable.getObjectName());
    }

    @Override
    public void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException
    {
        Path location = getDataTableLocation(identifier);
        final SchemaManager schemaManager = schemaManager(identifier, location);
        // first commit changes to underlying files
        TableSchema schema = schemaManager.commitChanges(changes);

        try {
            // sync to hive hms
            Optional<Table> table = metastore.getTable(identifier.getDatabaseName(), identifier.getObjectName());
            if (table.isEmpty()) {
                throw new TableNotExistException(identifier);
            }
            Table.Builder builder = Table.builder(table.get());
            updateHmsTable(builder, schema, location);
            Table newTable = builder.build();
            metastore.replaceTable(identifier.getDatabaseName(), identifier.getObjectName(), newTable, NO_PRIVILEGES);
        }
        catch (Exception te) {
            schemaManager.deleteSchema(schema.id());
            throw new RuntimeException(te);
        }
    }

    private void updateHmsTable(Table.Builder builder, TableSchema schema, Path location)
    {
        CoreOptions options = new CoreOptions(schema.options());
        if (options.partitionedTableInMetastore() && !schema.partitionKeys().isEmpty()) {
            Map<String, DataField> fieldMap = schema.fields().stream().collect(Collectors.toMap(DataField::name, Function.identity()));
            List<Column> partitionFields = schema.partitionKeys().stream().map(key -> this.convertToFieldSchema(fieldMap.get(key))).toList();
            builder.setPartitionColumns(partitionFields);

            Set<String> partitionKeys = new HashSet<>(schema.partitionKeys());
            List<Column> dataColumns = schema.fields().stream().filter(field -> !partitionKeys.contains(field.name())).map(this::convertToFieldSchema).toList();
            builder.setDataColumns(dataColumns);
        }
        else {
            if (options.tagToPartitionField() != null) {
                checkArgument(schema.partitionKeys().isEmpty(), "Partition table can not use timeTravelToPartitionField.");
                builder.setPartitionColumns(List.of(convertToFieldSchema(new DataField(0, options.tagToPartitionField(), DataTypes.STRING()))));
            }

            builder.setDataColumns(schema.fields().stream().map(this::convertToFieldSchema).toList());
        }

        convertToPropertiesPrefixKey(schema.options(), HIVE_PREFIX).forEach(builder::setParameter);

        if (schema.comment() != null) {
            builder.setParameter(COMMENT_PROP, schema.comment());
        }

        builder.withStorage(storage -> storage.setLocation(location.toString()));
    }

    @Override
    public TableSchema getDataTableSchema(Identifier identifier)
            throws TableNotExistException
    {
        if (!tableExists(identifier)) {
            throw new TableNotExistException(identifier);
        }
        Path tableLocation = getDataTableLocation(identifier);
        return new SchemaManager(fileIO, tableLocation)
                .latest()
                .orElseThrow(
                        () -> new RuntimeException("There is no paimon table in " + tableLocation));
    }

    @Override
    public boolean caseSensitive()
    {
        return false;
    }

    @Override
    public void close()
            throws Exception
    {
    }

    private Map<String, String> convertToProperties(Database database)
    {
        Map<String, String> properties = new HashMap<>(database.getParameters());
        if (database.getLocation() != null && database.getLocation().isPresent()) {
            properties.put(DB_LOCATION_PROP, database.getLocation().get());
        }
        if (database.getComment() != null && database.getComment().isPresent()) {
            properties.put(COMMENT_PROP, database.getComment().get());
        }
        return properties;
    }

    private static boolean isPaimonTable(Table table)
    {
        return PAIMON_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP))
                || SERDE_CLASS_NAME.equals(table.getStorage().getStorageFormat().getSerDeNullable());
    }

    private SchemaManager schemaManager(Identifier identifier, Path location)
    {
        return new SchemaManager(fileIO, location)
                .withLock(lock(identifier));
    }

    private Column convertToFieldSchema(DataField dataField)
    {
        requireNonNull(dataField, "dataField is null");
        return new Column(dataField.name(), HiveType.valueOf(HiveTypeUtils.toTypeInfo(dataField.type()).getTypeName().toLowerCase(ENGLISH)), Optional.ofNullable(dataField.description()), ImmutableMap.of());
    }

    private Lock lock(Identifier identifier)
    {
        if (!lockEnabled()) {
            return new Lock.EmptyLock();
        }

        TrinoHiveCatalogLock lock = new TrinoHiveCatalogLock(thriftMetastore, session);
        return Lock.fromCatalog(lock, identifier);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, String user, Optional<Path> location, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException
    {
        if (isSystemTable(identifier)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot '%s' for system table '%s', please use data table.",
                            "createTable", identifier));
        }
        validateCaseInsensitive(caseSensitive(), "Database", identifier.getDatabaseName());
        validateCaseInsensitive(caseSensitive(), "Table", identifier.getObjectName());
        validateCaseInsensitive(caseSensitive(), "Field", schema.rowType().getFieldNames());
        Map<String, String> options = schema.options();
        Preconditions.checkArgument(
                !Boolean.parseBoolean(
                        options.getOrDefault(
                                CoreOptions.AUTO_CREATE.key(),
                                CoreOptions.AUTO_CREATE.defaultValue().toString())),
                String.format(
                        "The value of %s property should be %s.",
                        CoreOptions.AUTO_CREATE.key(), Boolean.FALSE));

        if (!databaseExists(identifier.getDatabaseName())) {
            throw new DatabaseNotExistException(identifier.getDatabaseName());
        }

        if (tableExists(identifier)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(identifier);
        }

        copyTableDefaultOptions(schema.options());

        Path tableLocation = location.orElseGet(() -> getDataTableLocation(identifier));
        checkTableEmpty(identifier, tableLocation);
        TableSchema tableSchema;
        try {
            tableSchema = schemaManager(identifier, tableLocation).createTable(schema, false);

            Table.Builder builder = Table.builder()
                    .setDatabaseName(identifier.getDatabaseName())
                    .setTableName(identifier.getObjectName())
                    .setOwner(isUsingSystemSecurity ? Optional.empty() : Optional.of(user))
                    // Table needs to be EXTERNAL, otherwise table rename in HMS would rename table directory and break table contents.
                    .setTableType(EXTERNAL_TABLE.name())
                    .withStorage(storage -> storage.setStorageFormat(PAIMON_METASTORE_STORAGE_FORMAT))
                    // This is a must-have property for the EXTERNAL_TABLE table type
                    .setParameter("EXTERNAL", "TRUE")
                    .setParameter(TABLE_TYPE_PROP, PAIMON_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                    .setParameter(META_TABLE_STORAGE, STORAGE_HANDLER_CLASS_NAME);

            updateHmsTable(builder, tableSchema, tableLocation);

            Table table = builder.build();
            metastore.createTable(table, NO_PRIVILEGES);
        }
        catch (Exception e) {
            // clean up metadata files corresponding to the current transaction
            try {
                fileIO.deleteDirectoryQuietly(tableLocation);
            }
            catch (Exception ee) {
                LOG.error(ee, "Delete directory[%s] fail for table %s", tableLocation, identifier);
            }
            throw new TrinoException(PAIMON_COMMIT_ERROR, format("Failed to commit table %s.", identifier.getFullName()), e);
        }
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties, TrinoPrincipal owner)
            throws DatabaseAlreadyExistException
    {
        if (isSystemDatabase(name)) {
            throw new ProcessSystemDatabaseException();
        }
        if (databaseExists(name)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(name);
        }

        Database.Builder builder = Database.builder()
                .setDatabaseName(name)
                .setOwnerType(isUsingSystemSecurity ? Optional.empty() : Optional.of(owner.getType()))
                .setOwnerName(isUsingSystemSecurity ? Optional.empty() : Optional.of(owner.getName()));

        Map<String, String> parameter = new HashMap<>();
        properties.forEach(
                (key, value) -> {
                    if (key.equals(COMMENT_PROP)) {
                        builder.setComment(Optional.of(value));
                    }
                    else if (key.equals(DB_LOCATION_PROP)) {
                        builder.setLocation(Optional.of(value));
                    }
                    else if (value != null) {
                        parameter.put(key, value);
                    }
                });
        builder.setParameters(parameter);

        metastore.createDatabase(builder.build());
    }

    private boolean isSystemDatabase(String database)
    {
        return SYSTEM_DATABASE_NAME.equals(database);
    }

    private void checkTableEmpty(Identifier identifier, Path location)
    {
        try {
            FileStatus[] status = fileIO.listStatus(location);
            if (status.length != 0) {
                throw new TrinoException(
                        PAIMON_FILESYSTEM_ERROR,
                        format("Cannot create table %s on a non-empty location: %s", identifier.getFullName(), location));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
