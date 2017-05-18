module Purview
  module Databases
    class Base
      attr_reader :name, :tables

      def initialize(name, opts={})
        @name = name.to_sym
        @opts = opts
        @tables = Set.new.tap do |result|
          (default_tables + tables_opt).each do |table|
            table.database = self if result.add?(table)
          end
        end
      end

      def baseline_table(table, timestamp=Time.now.utc)
        ensure_table_valid_for_database(table)
        raise Purview::Exceptions::CouldNotBaselineTable.new(table) \
          unless table_initialized?(table)
        table_name = table_name(table)
        with_context_logging("`baseline_table` for: #{table_name}") do
          starting_timestamp = timestamp
          with_table_locked(table, starting_timestamp) do
            loop do
              last_window = sync_table_without_lock(table, timestamp)
              break if last_window.max > starting_timestamp
            end
          end
        end
        table_name
      end

      def create_index(index, opts={})
        ensure_index_valid_for_database(index)
        table_opts = extract_table_opts(opts)
        table_name = table_name(index.table, table_opts)
        index_opts = extract_index_opts(opts)
        index_name = index_name(
          table_name,
          index,
          index_opts
        )
        with_context_logging("`create_index` for: #{index_name}") do
          with_new_or_existing_connection(opts) do |connection|
            connection.execute(
              create_index_sql(
                table_name,
                index_name,
                index,
                index_opts
              )
            )
          end
        end
        index_name
      end

      def create_table(table, opts={})
        ensure_table_valid_for_database(table)
        ensure_table_metadata_exists_for_table(table)
        table_opts = extract_table_opts(opts)
        table_name = table_name(table, table_opts)
        with_context_logging("`create_table` for: #{table_name}") do
          with_new_or_existing_connection(opts) do |connection|
            connection.execute(
              send(
                create_table_sql_method_name(table, table_opts),
                table_name,
                table,
                table_opts
              )
            )
            if table_opts[:create_indices]
              table.indices.each do |index|
                create_index(
                  index,
                  :connection => connection,
                  :table => { :name => table_name }
                )
              end
            end
          end
        end
        table_name
      end

      def disable_table(table, timestamp=Time.now.utc)
        ensure_table_valid_for_database(table)
        table_name = table_name(table)
        with_context_logging("`disable_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(disable_table_sql(table)).rows_affected
            raise Purview::Exceptions::CouldNotDisableTable.new(table) \
              if zero?(rows_affected)
          end
        end
        table_name
      end

      def drop_index(index, opts={})
        ensure_index_valid_for_database(index)
        table_opts = extract_table_opts(opts)
        table_name = table_name(index.table, table_opts)
        index_opts = extract_index_opts(opts)
        index_name = index_name(
          table_name,
          index,
          index_opts
        )
        with_context_logging("`drop_index` for: #{index_name}") do
          with_new_or_existing_connection(opts) do |connection|
            connection.execute(
              drop_index_sql(
                table_name,
                index_name,
                index,
                index_opts
              )
            )
          end
        end
        index_name
      end

      def drop_table(table, opts={})
        ensure_table_valid_for_database(table)
        ensure_table_metadata_absent_for_table(table)
        table_opts = extract_table_opts(opts)
        table_name = table_name(table, table_opts)
        with_context_logging("`drop_table` for: #{table_name}") do
          with_new_connection do |connection|
            connection.execute(
              drop_table_sql(
                table_name,
                table,
                table_opts
              )
            )
          end
        end
        table_name
      end

      def enable_table(table, timestamp=Time.now.utc)
        ensure_table_valid_for_database(table)
        table_name = table_name(table)
        with_context_logging("`enable_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(enable_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotEnableTable.new(table) \
              if zero?(rows_affected)
          end
        end
        table_name
      end

      def initialize_table(table, timestamp=Time.now.utc)
        ensure_table_valid_for_database(table)
        table_name = table_name(table)
        with_context_logging("`initialize_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(initialize_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotInitializeTable.new(table) \
              if zero?(rows_affected)
          end
        end
        table_name
      end

      def lock_table(table, timestamp=Time.now.utc)
        ensure_table_valid_for_database(table)
        table_name = table_name(table)
        with_context_logging("`lock_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(lock_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotLockTable.new(table) \
              if zero?(rows_affected)
          end
        end
        table_name
      end

      def sync
        with_context_logging('`sync`') do
          with_timestamp do |timestamp|
            with_next_table(timestamp) do |table|
              sync_table_with_lock(table, timestamp)
            end
          end
        end
      end

      def sync_table(table, timestamp=Time.now.utc)
        ensure_table_valid_for_database(table)
        raise Purview::Exceptions::CouldNotSyncTable.new(table) \
          unless table_initialized?(table) && table_enabled?(table)
        table_name = table_name(table)
        with_context_logging("`sync_table` for: #{table_name}") do
          sync_table_with_lock(table, timestamp)
        end
        table_name
      end

      def table_metadata(table)
        ensure_table_valid_for_database(table)
        table_metadata = nil
        table_name = table_name(table)
        with_context_logging("`table_metadata` for: #{table_name}") do
          with_new_connection do |connection|
            table_metadata = Purview::Structs::TableMetadata.new(
              table_metadata_table.columns.reduce({}) do |memo, column|
                memo[column.name] = get_table_metadata_value(
                  connection,
                  table,
                  column
                )
                memo
              end
            )
          end
        end
        table_metadata
      end

      def unlock_table(table)
        ensure_table_valid_for_database(table)
        table_name = table_name(table)
        with_context_logging("`unlock_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(unlock_table_sql(table)).rows_affected
            raise Purview::Exceptions::CouldNotUnlockTable.new(table) \
              if zero?(rows_affected)
          end
        end
        table_name
      end

      private

      include Purview::Mixins::Connection
      include Purview::Mixins::Dialect
      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger

      attr_reader :opts

      public :connect

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

      def column_names(index_or_table)
        index_or_table.columns.map(&:name)
      end

      def column_definitions(index_or_table)
        index_or_table.columns.map { |column| column_definition(column) }
      end

      def connection_type
        raise %{All "#{Base}(s)" must override the "connection_type" method}
      end

      def create_index_sql(table_name, index_name, index, index_opts={})
        raise %{All "#{Base}(s)" must override the "create_index_sql" method}
      end

      def create_table_sql(table_name, table, table_opts={})
        raise %{All "#{Base}(s)" must override the "create_table_sql" method}
      end

      def create_table_sql_method_name(table, table_opts={})
        "create#{table_opts[:temporary] && '_temporary'}_table_sql".to_sym
      end

      def create_temporary_table_sql(table_name, table, table_opts={})
        raise %{All "#{Base}(s)" must override the "create_temporary_table_sql" method}
      end

      def database_host
        opts[:database_host]
      end

      def database_name
        name
      end

      def database_password
        opts[:database_password]
      end

      def database_port
        opts[:database_port]
      end

      def database_username
        opts[:database_username]
      end

      def default(column)
        column.default || default_map[column.type]
      end

      def default_map
        {}
      end

      def default_tables
        []
      end

      def dialect_type
        raise %{All "#{Base}(s)" must override the "dialect_type" method}
      end

      def disable_table_sql(table)
        raise %{All "#{Base}(s)" must override the "disable_table_sql" method}
      end

      def drop_index_sql(table_name, index_name, index, index_opts={})
        raise %{All "#{Base}(s)" must override the "drop_index_sql" method}
      end

      def drop_table_sql(table_name, table, table_opts={})
        raise %{All "#{Base}(s)" must override the "drop_table_sql" method}
      end

      def enable_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "enable_table_sql" method}
      end

      def ensure_index_valid_for_database(index)
        raise ArgumentError.new('Must provide an `Index`') \
          unless index
        ensure_table_valid_for_database(index.table)
      end

      def ensure_table_metadata_absent_for_table(table)
        with_new_connection do |connection|
          connection.execute(ensure_table_metadata_table_exists_sql)
          connection.execute(ensure_table_metadata_absent_for_table_sql(table))
        end
      end

      def ensure_table_metadata_absent_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "ensure_table_metadata_absent_for_table_sql" method}
      end

      def ensure_table_metadata_exists_for_table(table)
        with_new_connection do |connection|
          connection.execute(ensure_table_metadata_table_exists_sql)
          connection.execute(ensure_table_metadata_exists_for_table_sql(table))
        end
      end

      def ensure_table_metadata_exists_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "ensure_table_metadata_exists_for_table_sql" method}
      end

      def ensure_table_metadata_table_exists_sql
        raise %{All "#{Base}(s)" must override the "ensure_table_metadata_table_exists_sql" method}
      end

      def ensure_table_valid_for_database(table)
        raise ArgumentError.new('Must provide a `Table`') \
          unless table
        raise Purview::Exceptions::WrongDatabaseForTable.new(table) \
          unless tables.include?(table)
      end

      def extract_index_opts(opts)
        opts[:index] || {}
      end

      def extract_table_opts(opts)
        opts[:table] || { :create_indices => true }
      end

      def get_table_metadata_value(connection, table, column)
        row = connection.execute(get_table_metadata_value_sql(table, column)).rows[0]
        raise CouldNotFindMetadataForTable.new(table) \
          unless row
        value = row[column.name]
        value && column.type.parse(value)
      end

      def index_name(table_name, index, index_opts={})
        index_opts[:name] || 'index_%s_on_%s' % [
          table_name,
          column_names(index).join('_and_'),
        ]
      end

      def initialize_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "initialize_table_sql" method}
      end

      def limit(column)
        limitless_types.include?(column.type) ? nil : (column.limit || limit_map[column.type])
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
        row = connection.execute(next_table_sql(timestamp)).rows[0]
        value = row && row[table_metadata_table.table_name_column.name]
        value && table_metadata_table.table_name_column.type.parse(value)
      end

      def next_table_sql(timestamp)
        raise %{All "#{Base}(s)" must override the "next_table_sql" method}
      end

      def next_window(connection, table, timestamp)
        min = get_table_metadata_value(
          connection,
          table,
          table_metadata_table.max_timestamp_pulled_column
        )
        max = min + table.window_size
        now = timestamp
        min > now ? nil : Purview::Structs::Window.new(
          :min => min,
          :max => max > now ? now : max
        )
      end

      def nullable?(column)
        column.nullable?
      end

      def primary_key?(column)
        column.primary_key?
      end

      def set_table_metadata_value(connection, table, column, value)
        rows_affected = \
          connection.execute(set_table_metadata_value_sql(table, column, value)).rows_affected
        raise CouldNotUpdateMetadataForTable.new(table) \
          if zero?(rows_affected)
      end

      def sync_table_with_lock(table, timestamp)
        last_window = nil
        with_table_locked(table, timestamp) do
          last_window = sync_table_without_lock(table, timestamp)
        end
        last_window
      end

      def sync_table_without_lock(table, timestamp)
        last_window = nil
        with_next_window(table, timestamp) do |window|
          with_new_connection do |connection|
            with_transaction(connection) do
              table.sync(connection, window)
              set_table_metadata_value(
                connection,
                table,
                table_metadata_table.last_pulled_at_column,
                timestamp
              )
              set_table_metadata_value(
                connection,
                table,
                table_metadata_table.max_timestamp_pulled_column,
                window.max
              )
              last_window = window
            end
          end
        end
        last_window
      end

      def table_disabled?(table)
        !table_enabled?(table)
      end

      def table_enabled?(table)
        with_new_connection do |connection|
          !!get_table_metadata_value(
              connection,
              table,
              table_metadata_table.enabled_at_column
            )
        end
      end

      def table_initialized?(table)
        with_new_connection do |connection|
          !!get_table_metadata_value(
              connection,
              table,
              table_metadata_table.max_timestamp_pulled_column
            )
        end
      end

      def table_locked?(table)
        with_new_connection do |connection|
          !!get_table_metadata_value(
              connection,
              table,
              table_metadata_table.locked_at_column
            )
        end
      end

      def table_metadata_table
        @table_metadata_table ||= Purview::Tables::TableMetadata.new(self)
      end

      def table_name(table, table_opts={})
        table_opts[:name] || table.name
      end

      def table_unlocked?(table)
        !table_locked?(table)
      end

      def tables_by_name
        @tables_by_name ||= {}.tap do |result|
          tables.each do |table|
            result[table.name] = table
          end
        end
      end

      def tables_opt
        opts[:tables] || []
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

      def with_next_table(timestamp)
        with_new_connection do |connection|
          table = next_table(connection, timestamp)
          raise Purview::Exceptions::NoTable.new \
            unless table
          yield table
        end
      end

      def with_next_window(table, timestamp)
        with_new_connection do |connection|
          window = next_window(
            connection,
            table,
            timestamp
          )
          raise Purview::Exceptions::NoWindowForTable.new(table) \
            unless window
          yield window
        end
      end

      def with_table_locked(table, timestamp)
        lock_table(table, timestamp)
        yield
      ensure
        unlock_table(table)
      end
    end
  end
end
