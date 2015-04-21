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
          if table_opts[:create_indices]
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
      end

      def create_temporary_table(connection, table, opts={})
        table_opts = extract_table_options(opts)
        table_name(table, table_opts).tap do |table_name|
          connection.execute(
            create_temporary_table_sql(
              table_name,
              table,
              table_opts
            )
          )
          if table_opts[:create_indices]
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

      def lock_table(table, timestamp)
        with_context_logging("`lock_table` for: #{table_name(table)}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(lock_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotAcquireLock.new(table) \
              if zero?(rows_affected)
          end
        end
      end

      def null_value
        raise %{All "#{Base}(s)" must override the "null_value" method}
      end

      def quoted(value)
        value.nil? ? null_value : value.quoted
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
            raise Purview::Exceptions::LockAlreadyReleased.new(table) \
              if zero?(rows_affected)
          end
        end
      end

      private

      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger

      attr_reader :opts, :tables

      def column_names(columns)
        columns.map(&:name)
      end

      def column_definition(column)
        column.name.to_s.tap do |column_definition|
          type = type(column)
          column_definition << " #{type}"
          limit = limit(column)
          column_definition << "(#{limit})" if limit
          primary_key = primary_key?(column)
          column_definition << ' PRIMARY KEY' if primary_key
          nullable = nullable?(column)
          column_definition << " #{nullable ? 'NULL' : 'NOT NULL'}"
          default = default(column)
          column_definition << " DEFAULT #{default}" if default
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
        {}
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

      def create_temporary_table_sql(table_name, table, table_opts={})
        raise %{All "#{Base}(s)" must override the "create_temporary_table_sql" method}
      end

      def default(column)
        column.default || default_map[column.type]
      end

      def default_map
        {}
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

      def ensure_table_metadata_exists_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "ensure_table_metadata_exists_for_table_sql" method}
      end

      def ensure_table_metadata_table_exists_sql
        raise %{All "#{Base}(s)" must override the "ensure_table_metadata_table_exists_sql" method}
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
        opts[:table] || { :create_indices => true }
      end

      def get_enabled_for_table(connection, table)
        row = connection.execute(get_last_pulled_at_for_table_sql(table)).rows[0]
        enabled = row[table_metadata_enabled_column_name]
        !!(enabled =~ /\A(true|t|yes|y|1)\z/i)
      end

      def get_enabled_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "get_enabled_for_table_sql" method}
      end

      def get_last_pulled_at_for_table(connection, table)
        row = connection.execute(get_last_pulled_at_for_table_sql(table)).rows[0]
        timestamp = row[table_metadata_last_pulled_at_column_name]
        timestamp ? Time.parse(timestamp) : nil
      end

      def get_last_pulled_at_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "get_last_pulled_at_for_table_sql" method}
      end

      def get_locked_at_for_table(connection, table)
        row = connection.execute(get_locked_at_for_table_sql(table)).rows[0]
        timestamp = row[table_metadata_locked_at_column_name]
        timestamp ? Time.parse(timestamp) : nil
      end

      def get_locked_at_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "get_locked_at_for_table_sql" method}
      end

      def get_max_timestamp_pulled_for_table(connection, table)
        row = connection.execute(get_max_timestamp_pulled_for_table_sql(table)).rows[0]
        timestamp = row[table_metadata_max_timestamp_pulled_column_name]
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

      def limit(column)
        return nil if limitless_types.include?(column.type)
        column.limit || limit_map[column.type]
      end

      def limit_map
        {}
      end

      def limitless_types
        []
      end

      def lock_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "lock_table_sql" method}
      end

      def next_table(connection, timestamp)
        ensure_table_metadata_table_exists
        ensure_table_metadata_exists_for_tables
        row = connection.execute(next_table_sql(timestamp)).rows[0]
        table_name = row && row[table_metadata_table_name_column_name]
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
        Purview::Structs::Window.new(:min => min, :max => max)
      end

      def nullable?(column)
        column.nullable?
      end

      def primary_key?(column)
        column.primary_key?
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
        raise %{All "#{Base}(s)" must override the "set_last_pulled_at_for_table_sql" method}
      end

      def set_locked_at_for_table(connection, table, timestamp)
        connection.execute(set_locked_at_for_table_sql(table, timestamp))
      end

      def set_locked_at_for_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "set_locked_at_for_table_sql" method}
      end

      def set_max_timestamp_pulled_for_table(connection, table, timestamp)
        connection.execute(set_max_timestamp_pulled_for_table_sql(table, timestamp))
      end

      def set_max_timestamp_pulled_for_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "set_max_timestamp_pulled_for_table_sql" method}
      end

      def table_metadata_enabled_column_definition
        column = Purview::Columns::Boolean.new(table_metadata_enabled_column_name)
        column_definition(column)
      end

      def table_metadata_enabled_column_name
        'enabled'
      end

      def table_metadata_last_pulled_at_column_definition
        column = Purview::Columns::Timestamp.new(table_metadata_last_pulled_at_column_name)
        column_definition(column)
      end

      def table_metadata_last_pulled_at_column_name
        'last_pulled_at'
      end

      def table_metadata_locked_at_column_definition
        column = Purview::Columns::Timestamp.new(table_metadata_locked_at_column_name)
        column_definition(column)
      end

      def table_metadata_locked_at_column_name
        'locked_at'
      end

      def table_metadata_max_timestamp_pulled_column_definition
        column = Purview::Columns::Timestamp.new(table_metadata_max_timestamp_pulled_column_name)
        column_definition(column)
      end

      def table_metadata_max_timestamp_pulled_column_name
        'max_timestamp_pulled'
      end

      def table_metadata_table_name
        'table_metadata'
      end

      def table_metadata_table_name_column_definition
        column = Purview::Columns::String.new(table_metadata_table_name_column_name)
        column_definition(column)
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

      def type(column)
        type_map[column.type]
      end

      def type_map
        {
          Purview::Types::Boolean => 'boolean',
          Purview::Types::Date => 'date',
          Purview::Types::Float => 'float',
          Purview::Types::Integer => 'integer',
          Purview::Types::String => 'varchar',
          Purview::Types::Text => 'text',
          Purview::Types::Time => 'time',
          Purview::Types::Timestamp => 'timestamp',
        }
      end

      def unlock_table_sql(table)
        raise %{All "#{Base}(s)" must override the "unlock_table_sql" method}
      end

      def with_new_connection
        yield connection = connect
      ensure
        connection.disconnect if connection
      end

      def with_next_table(connection, timestamp)
        table = next_table(connection, timestamp)
        raise Purview::Exceptions::NoTable.new unless table
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
        raise Purview::Exceptions::NoWindow.new(table) unless window
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
