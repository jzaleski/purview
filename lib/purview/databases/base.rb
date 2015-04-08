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
        'FALSE'
      end

      def lock_table(table, timestamp=Time.now.utc)
        with_context_logging("`lock_table` for: #{table_name(table)}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(lock_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotAcquireLockException.new(table) \
              if rows_affected.zero?
          end
        end
      end

      def null_value
        'NULL'
      end

      def quoted(value)
        value.nil? ? null_value : "'#{value}'"
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
        'TRUE'
      end

      def unlock_table(table)
        with_context_logging("`unlock_table` for: #{table_name(table)}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(unlock_table_sql(table)).rows_affected
            raise Purview::Exceptions::LockAlreadyReleasedException.new(table) \
              if rows_affected.zero?
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
        column.name.to_s.tap do |column_definition|
          column_definition << " #{database_type(column)}"
          column_definition << "(#{column.limit})" if column.limit? && !limitless_types.include?(column)
          column_definition << ' PRIMARY KEY' if column.primary_key?
          column_definition << ' NOT NULL' unless column.allow_blank?
          column_definition << " DEFAULT #{column.default}" if column.default?
        end
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
        database_type_map[column.class]
      end

      def database_type_map
        raise %{All "#{Base}(s)" must override the "database_type_map" method}
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

      def ensure_table_metadata_table_exists
        with_new_connection do |connection|
          connection.execute(ensure_table_metadata_table_exists_sql)
        end
      end

      def ensure_table_metadata_table_exists_sql
        'CREATE TABLE IF NOT EXISTS %s (%s %s PRIMARY KEY, %s %s, %s %s, %s %s, %s %s)' % [
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          table_metadata_table_name_column_type,
          table_metadata_enabled_column_name,
          table_metadata_enabled_column_type,
          table_metadata_last_pulled_at_column_name,
          table_metadata_last_pulled_at_column_type,
          table_metadata_locked_at_column_name,
          table_metadata_locked_at_column_type,
          table_metadata_max_timestamp_pulled_column_name,
          table_metadata_max_timestamp_pulled_column_type,
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
        row = connection.execute(get_last_pulled_at_for_table_sql(table)).rows[0]
        enabled = row[table_metadata_enabled_column_name]
        !!(enabled =~ /\A(true|t|yes|y|1)\z/)
      end

      def get_enabled_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_enabled_column_name,
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def get_last_pulled_at_for_table(connection, table)
        row = connection.execute(get_last_pulled_at_for_table_sql(table)).rows[0]
        timestamp = row[table_metadata_last_pulled_at_column_name]
        timestamp ? Time.parse(timestamp) : nil
      end

      def get_last_pulled_at_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_last_pulled_at_column_name,
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def get_locked_at_for_table(connection, table)
        row = connection.execute(get_locked_at_for_table_sql(table)).rows[0]
        timestamp = row[table_metadata_locked_at_column_name]
        timestamp ? Time.parse(timestamp) : nil
      end

      def get_locked_at_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_locked_at_column_name,
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def get_max_timestamp_pulled_for_table(connection, table)
        row = connection.execute(get_max_timestamp_pulled_for_table_sql(table)).rows[0]
        timestamp = row[table_metadata_max_timestamp_pulled_column_name]
        timestamp ? Time.parse(timestamp) : table.starting_timestamp
      end

      def get_max_timestamp_pulled_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_max_timestamp_pulled_column_name,
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def index_name(table_name, columns, index_opts={})
        index_opts[:name] || 'index_%s_on_%s' % [
          table_name,
          column_names(columns).join('_and_'),
        ]
      end

      def limitless_types
        Set.new
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

      def next_table(connection, timestamp)
        ensure_table_metadata_table_exists
        ensure_table_metadata_exists_for_tables
        row = connection.execute(next_table_sql(timestamp)).rows[0]
        table_name = row && row[table_metadata_table_name_column_name]
        table_name ? tables_by_name[table_name] : nil
      end

      def next_table_sql(timestamp)
        'SELECT %s FROM %s WHERE %s = %s AND %s IS NULL ORDER BY %s IS NULL DESC, %s LIMIT 1' % [
          table_metadata_table_name_column_name,
          table_metadata_table_name,
          table_metadata_enabled_column_name,
          quoted(true_value),
          table_metadata_locked_at_column_name,
          table_metadata_last_pulled_at_column_name,
          table_metadata_last_pulled_at_column_name,
        ]
      end

      def next_window(connection, table, timestamp)
        min = get_max_timestamp_pulled_for_table(connection, table)
        max = min + table.window_size
        now = timestamp
        return nil if min > now
        max = now if max > now
        Purview::Structs::Window.new(:min => min, :max => max)
      end

      def set_enabled_for_table(connection, table, enabled)
        connection.execute(set_enabled_for_table_sql(table, enabled))
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

      def set_last_pulled_at_for_table(connection, table, timestamp)
        connection.execute(set_last_pulled_at_for_table_sql(table, timestamp))
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

      def set_locked_at_for_table(connection, table, timestamp)
        connection.execute(set_locked_at_for_table_sql(table, timestamp))
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

      def set_max_timestamp_pulled_for_table(connection, table, timestamp)
        connection.execute(set_max_timestamp_pulled_for_table_sql(table, timestamp))
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

      def table_metadata_enabled_column_name
        'enabled'
      end

      def table_metadata_enabled_column_type
        database_type_map[Purview::Columns::Boolean]
      end

      def table_metadata_last_pulled_at_column_name
        'last_pulled_at'
      end

      def table_metadata_last_pulled_at_column_type
        database_type_map[Purview::Columns::Boolean]
      end

      def table_metadata_locked_at_column_name
        'locked_at'
      end

      def table_metadata_locked_at_column_type
        database_type_map[Purview::Columns::Timestamp]
      end

      def table_metadata_max_timestamp_pulled_column_name
        'max_timestamp_pulled'
      end

      def table_metadata_max_timestamp_pulled_column_type
        database_type_map[Purview::Columns::Timestamp]
      end

      def table_metadata_table_name
        'table_metadata'
      end

      def table_metadata_table_name_column_name
        'table_name'
      end

      def table_metadata_table_name_column_type
        database_type_map[Purview::Columns::String]
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
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NOT NULL' % [
          table_metadata_table_name,
          table_metadata_locked_at_column_name,
          null_value,
          table_metadata_table_name_column_name,
          quoted(table.name),
          table_metadata_locked_at_column_name,
        ]
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
  end
end
