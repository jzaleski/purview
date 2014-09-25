module Purview
  module Databases
    class Base
      attr_reader :name

      def initialize(name, opts={})
        @name = name
        @opts = opts
        @tables = Set.new
      end

      def add_table(table)
        @tables << table
      end

      def connect
        connection.connect
      end

      def create_index(connection, table, columns, opts={})
        table_opts = extract_table_options(opts)
        table_name = table_name(table, table_opts)
        index_opts = extract_index_options(opts)
        index_name(
          table_name,
          columns,
          index_opts
        ).tap do |index_name|
          connection.execute(
            create_index_sql(
              table_name,
              index_name,
              table,
              columns,
              index_opts
            )
          )
        end
      end

      def create_table(connection, table, opts={})
        table_opts = extract_table_options(opts)
        table_name(table, table_opts).tap do |table_name|
          connection.execute(
            create_table_sql(
              table_name,
              table,
              table_opts
            )
          )
          table.indexed_columns.each do |columns|
            create_index(
              connection,
              table,
              columns,
              :table => { :name => table_name }
            )
          end
        end
      end

      def disable_table(table)
        with_context_logging("`disable_table` for: #{table_name(table)}") do
          with_new_connection do |connection|
            set_enabled_for_table(
              connection,
              table,
              false_value
            )
          end
        end
      end

      def drop_table(connection, table, opts={})
        table_opts = extract_table_options(opts)
        table_name(table, table_opts).tap do |table_name|
          connection.execute(
            drop_table_sql(
              table_name,
              table,
              table_opts
            )
          )
        end
      end

      def drop_index(connection, table, columns, opts={})
        table_opts = extract_table_options(opts)
        table_name = table_name(table, table_opts)
        index_opts = extract_index_options(opts)
        index_name(
          table_name,
          columns,
          index_opts
        ).tap do |index_name|
          connection.execute(
            drop_index_sql(
              table_name,
              index_name,
              table,
              columns,
              index_opts
            )
          )
        end
      end

      def enable_table(table)
        with_context_logging("`enable_table` for: #{table_name(table)}") do
          with_new_connection do |connection|
            set_enabled_for_table(
              connection,
              table,
              true_value
            )
          end
        end
      end

      def false_value
        raise %{All "#{Base}(s)" must override the "false_value" method}
      end

      def lock_table(table, timestamp=Time.now.utc)
        with_context_logging("`lock_table` for: #{table_name(table)}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(lock_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotAcquireLockException.new(table) \
              if rows_affected == 0
          end
        end
      end

      def null_value
        raise %{All "#{Base}(s)" must override the "null_value" method}
      end

      def quoted(value)
        raise %{All "#{Base}(s)" must override the "quoted" method}
      end

      def sync
        with_new_connection do |connection|
          with_transaction(connection) do |timestamp|
            with_next_table(connection, timestamp) do |table|
              with_next_window(
                connection,
                table,
                timestamp
              ) do |window|
                with_table_locked(table, timestamp) do
                  table.sync(connection, window)
                end
              end
            end
          end
        end
      end

      def true_value
        raise %{All "#{Base}(s)" must override the "true_value" method}
      end

      def unlock_table(table)
        with_context_logging("`unlock_table` for: #{table_name(table)}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(unlock_table_sql(table)).rows_affected
            raise Purview::Exceptions::LockAlreadyReleasedException.new(table) \
              if rows_affected == 0
          end
        end
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts, :tables

      def column_names(columns)
        columns.map(&:name)
      end

      def column_definition(column)
        raise %{All "#{Base}(s)" must override the "column_definition" method}
      end

      def column_definitions(table)
        [].tap do |results|
          results << column_definition(table.id_column)
          results << column_definition(table.created_timestamp_column)
          results << column_definition(table.updated_timestamp_column)
          table.data_columns.each do |column|
            results << column_definition(column)
          end
        end
      end

      def connection
        connection_type.new(connection_opts)
      end

      def connection_opts
        raise %{All "#{Base}(s)" must override the "connection_opts" method}
      end

      def connection_type
        raise %{All "#{Base}(s)" must override the "connection_type" method}
      end

      def create_index_sql(table_name, index_name, table, columns, index_opts={})
        raise %{All "#{Base}(s)" must override the "create_index_sql" method}
      end

      def create_table_sql(table_name, table, table_opts={})
        raise %{All "#{Base}(s)" must override the "create_table_sql" method}
      end

      def database_type(column)
        raise %{All "#{Base}(s)" must override the "database_type" method}
      end

      def database_type_map
        raise %{All "#{Base}(s)" must override the "database_type_map" method}
      end

      def drop_index_sql(table_name, index_name, table, columns, index_opts={})
        raise %{All "#{Base}(s)" must override the "drop_index_sql" method}
      end

      def drop_table_sql(table_name, table, table_opts={})
        raise %{All "#{Base}(s)" must override the "drop_table_sql" method}
      end

      def ensure_table_metadata_table_exists
        with_new_connection do |connection|
          connection.execute(ensure_table_metadata_table_exists_sql)
        end
      end

      def ensure_table_metadata_exists_for_tables
        with_new_connection do |connection|
          tables.each do |table|
            connection.execute(ensure_table_metadata_exists_for_table_sql(table))
          end
        end
      end

      def extract_index_options(opts)
        opts[:index] || {}
      end

      def extract_table_options(opts)
        opts[:table] || {}
      end

      def get_enabled_for_table(connection, table)
        result = connection.execute(get_last_pulled_at_for_table_sql(table)).data[0]
        enabled = result[table_metadata_enabled_column_name]
        !!(enabled =~ /\A(true|t|yes|y|1)\z/)
      end

      def get_enabled_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "get_enabled_for_table_sql" method}
      end

      def get_last_pulled_at_for_table(connection, table)
        result = connection.execute(get_last_pulled_at_for_table_sql(table)).data[0]
        timestamp = result[table_metadata_last_pulled_at_column_name]
        timestamp ? Time.parse(timestamp) : nil
      end

      def get_last_pulled_at_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "get_last_pulled_at_for_table_sql" method}
      end

      def get_locked_at_for_table(connection, table)
        result = connection.execute(get_locked_at_for_table_sql(table)).data[0]
        timestamp = result[table_metadata_locked_at_column_name]
        timestamp ? Time.parse(timestamp) : nil
      end

      def get_locked_at_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "get_locked_at_for_table_sql" method}
      end

      def get_max_timestamp_pulled_for_table(connection, table)
        result = connection.execute(get_max_timestamp_pulled_for_table_sql(table)).data[0]
        timestamp = result[table_metadata_max_timestamp_pulled_column_name]
        timestamp ? Time.parse(timestamp) : table.starting_timestamp
      end

      def get_max_timestamp_pulled_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "get_max_timestamp_pulled_for_table_sql" method}
      end

      def index_name(table_name, columns, index_opts={})
        index_opts[:name] || 'index_%s_on_%s' % [
          table_name,
          column_names(columns).join('_and_'),
        ]
      end

      def lock_table_sql(table, timestamp)
        # NOTE: is important to ensure that the this will only attempt to lock a
        # table that is not currently locked
        raise %{All "#{Base}(s)" must override the "lock_table_sql" method}
      end

      def next_table(connection, timestamp)
        ensure_table_metadata_table_exists
        ensure_table_metadata_exists_for_tables
        result = connection.execute(next_table_sql(timestamp)).data[0]
        table_name = result && result[table_metadata_table_name_column_name]
        table_name ? tables_by_name[table_name] : nil
      end

      def next_table_sql(timestamp)
        raise %{All "#{Base}(s)" must override the "next_table_sql" method}
      end

      def next_window(connection, table, timestamp)
        min = get_max_timestamp_pulled_for_table(connection, table)
        max = min + table.window_size
        now = timestamp
        return nil if min > now
        max = now if max > now
        OpenStruct.new(:min => min, :max => max)
      end

      def set_enabled_for_table(connection, table, enabled)
        connection.execute(set_enabled_for_table_sql(table, enabled))
      end

      def set_enabled_for_table_sql(table, enabled)
        raise %{All "#{Base}(s)" must override the "set_enabled_for_table_sql" method}
      end

      def set_last_pulled_at_for_table(connection, table, timestamp)
        connection.execute(set_last_pulled_at_for_table_sql(table, timestamp))
      end

      def set_last_pulled_at_for_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "get_last_pulled_at_for_table_sql" method}
      end

      def set_locked_at_for_table(connection, table, timestamp)
        connection.execute(set_locked_at_for_table_sql(table, timestamp))
      end

      def set_locked_at_for_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "get_locked_at_for_table_sql" method}
      end

      def set_max_timestamp_pulled_for_table(connection, table, timestamp)
        connection.execute(set_max_timestamp_pulled_for_table_sql(table, timestamp))
      end

      def set_max_timestamp_pulled_for_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "get_max_timestamp_pulled_for_table_sql" method}
      end

      def table_metadata_enabled_column_name
        'enabled'
      end

      def table_metadata_last_pulled_at_column_name
        'last_pulled_at'
      end

      def table_metadata_locked_at_column_name
        'locked_at'
      end

      def table_metadata_max_timestamp_pulled_column_name
        'max_timestamp_pulled'
      end

      def table_metadata_table_name
        'table_metadata'
      end

      def table_metadata_table_name_column_name
        'table_name'
      end

      def tables_by_name
        {}.tap do |result|
          tables.each do |table|
            result[table.name] = table
          end
        end
      end

      def table_name(table, table_opts={})
        table_opts[:name] || table.name
      end

      def unlock_table_sql(table)
        # NOTE: is important to ensure that the this will only attempt to unlock
        # a table that is currently locked
        raise %{All "#{Base}(s)" must override the "unlock_table_sql" method}
      end

      def with_new_connection
        yield connection = connect
      ensure
        connection.disconnect if connection
      end

      def with_next_table(connection, timestamp)
        table = next_table(connection, timestamp)
        raise Purview::Exceptions::NoTableException.new unless table
        yield table
        set_last_pulled_at_for_table(
          connection,
          table,
          timestamp
        )
      end

      def with_next_window(connection, table, timestamp)
        window = next_window(
          connection,
          table,
          timestamp
        )
        raise Purview::Exceptions::NoWindowException.new(table) unless window
        yield window
        set_max_timestamp_pulled_for_table(
          connection,
          table,
          window.max
        )
      end

      def with_table_locked(table, timestamp)
        lock_table(table, timestamp)
        yield
      ensure
        unlock_table(table)
      end

      def with_transaction(connection)
        connection.with_transaction { yield Time.now.utc }
      end
    end

    class PostgreSQL < Base
      def false_value
        'FALSE'
      end

      def null_value
        'NULL'
      end

      def quoted(value)
        value.nil? ? null_value : "'#{value}'"
      end

      def true_value
        'TRUE'
      end

      private

      def column_definition(column)
        column.name.to_s.tap do |column_definition|
          column_definition << " #{database_type(column)}"
          column_definition << ' PRIMARY KEY' if column.primary_key?
          column_definition << "(#{column.limit})" if column.limit && !limitless_types.include?(column.type)
          column_definition << ' NOT NULL' unless column.allow_blank?
          column_definition << " DEFAULT #{column.default}" if column.default
        end
      end

      def connection_opts
        { :dbname => name }
      end

      def connection_type
        Purview::Connections::PostgreSQL
      end

      def create_index_sql(table_name, index_name, table, columns, index_opts={})
        'CREATE INDEX %s ON %s (%s)' % [
          index_name,
          table_name,
          column_names(columns).join(', '),
        ]
      end

      def create_table_sql(table_name, table, table_opts={})
        'CREATE'.tap do |result|
          result << ' TEMPORARY' if table_opts[:temporary]
          result << ' TABLE %s (%s)' % [
            table_name,
            column_definitions(table).join(', '),
          ]
        end
      end

      def database_type(column)
        database_type_map[column.type]
      end

      def database_type_map
        {
          Purview::Types::Boolean => 'boolean',
          Purview::Types::Date => 'date',
          Purview::Types::Float => 'numeric',
          Purview::Types::Integer => 'integer',
          Purview::Types::Money => 'money',
          Purview::Types::String => 'varchar',
          Purview::Types::Text => 'text',
          Purview::Types::Time => 'time',
          Purview::Types::Timestamp => 'timestamp',
          Purview::Types::UUID => 'uuid',
        }
      end

      def drop_index_sql(table_name, index_name, table, columns, index_opts={})
        'DROP INDEX %s' % [
          index_name,
        ]
      end

      def drop_table_sql(table_name, table, table_opts={})
        'DROP TABLE %s' % [
          table_name,
        ]
      end

      def ensure_table_metadata_table_exists_sql
        'CREATE TABLE IF NOT EXISTS %s (%s varchar(255) PRIMARY KEY, %s boolean, %s timestamp, %s timestamp, %s timestamp)' % [
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          table_metadata_enabled_column_name,
          table_metadata_last_pulled_at_column_name,
          table_metadata_locked_at_column_name,
          table_metadata_max_timestamp_pulled_column_name,
        ]
      end

      def ensure_table_metadata_exists_for_table_sql(table)
        'INSERT INTO %s (%s) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s = %s)' % [
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def get_enabled_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_enabled_column_name,
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def get_last_pulled_at_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_last_pulled_at_column_name,
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def get_locked_at_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_locked_at_column_name,
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def get_max_timestamp_pulled_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_max_timestamp_pulled_column_name,
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def limitless_types
        [Purview::Types::UUID]
      end

      def lock_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NULL' % [
          table_metadata_table_name,
          table_metadata_locked_at_column_name,
          quoted(timestamp),
          table_metadata_table_name_column_name,
          quoted(table.name),
          table_metadata_locked_at_column_name,
        ]
      end

      def next_table_sql(timestamp)
        'SELECT %s FROM %s WHERE %s = %s AND %s IS NULL ORDER BY %s NULLS FIRST LIMIT 1' % [
          table_metadata_table_name_column_name,
          table_metadata_table_name,
          table_metadata_enabled_column_name,
          quoted(true_value),
          table_metadata_locked_at_column_name,
          table_metadata_last_pulled_at_column_name,
        ]
      end

      def set_enabled_for_table_sql(table, enabled)
        'UPDATE %s SET %s = %s WHERE %s = %s AND (%s IS NULL OR %s = %s)' % [
          table_metadata_table_name,
          table_metadata_enabled_column_name,
          quoted(enabled),
          table_metadata_table_name_column_name,
          quoted(table.name),
          table_metadata_enabled_column_name,
          table_metadata_enabled_column_name,
          quoted(false_value),
        ]
      end

      def set_last_pulled_at_for_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s' % [
          table_metadata_table_name,
          table_metadata_last_pulled_at_column_name,
          quoted(timestamp),
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def set_locked_at_for_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s' % [
          table_metadata_table_name,
          table_metadata_locked_at_column_name,
          quoted(timestamp),
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def set_max_timestamp_pulled_for_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s' % [
          table_metadata_table_name,
          table_metadata_max_timestamp_pulled_column_name,
          quoted(timestamp),
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def unlock_table_sql(table)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NOT NULL' % [
          table_metadata_table_name,
          table_metadata_locked_at_column_name,
          null_value,
          table_metadata_table_name_column_name,
          quoted(table.name),
          table_metadata_locked_at_column_name,
        ]
      end
    end
  end
end
