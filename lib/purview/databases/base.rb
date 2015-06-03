module Purview
  module Databases
    class Base
      attr_reader :name

      def initialize(name, opts={})
        @name = name
        @opts = opts
      end

      def baseline_table(table)
        raise Purview::Exceptions::WrongDatabase.new(table) \
          unless tables.include?(table)
        table_name = table_name(table)
        with_context_logging("`baseline_table` for: #{table_name}") do
          starting_timestamp = Time.now.utc
          with_table_locked(table, starting_timestamp) do
            loop do
              timestamp = Time.now.utc
              last_window = sync_table_without_lock(table, timestamp)
              break if last_window.max > starting_timestamp
            end
          end
        end
      end

      def create_table(table, opts={})
        ensure_table_metadata_exists_for_table(table)
        table_opts = extract_table_options(opts)
        table_name = table_name(table, table_opts)
        with_context_logging("`create_table` for: #{table_name}") do
          with_new_connection do |connection|
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
          table_name
        end
      end

      def create_temporary_table(connection, table, opts={})
        table_opts = extract_table_options(opts)
        table_name = table_name(table, table_opts)
        with_context_logging("`create_temporary_table` for: #{table_name}") do
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
          table_name
        end
      end

      def disable_table(table)
        table_name = table_name(table)
        with_context_logging("`disable_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(disable_table_sql(table)).rows_affected
            raise Purview::Exceptions::CouldNotDisable.new(table) \
              if zero?(rows_affected)
          end
          table_name
        end
      end

      def drop_table(table, opts={})
        ensure_table_metadata_absent_for_table(table)
        table_opts = extract_table_options(opts)
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
          table_name
        end
      end

      def enable_table(table, timestamp=Time.now.utc)
        table_name = table_name(table)
        with_context_logging("`enable_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(enable_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotEnable.new(table) \
              if zero?(rows_affected)
          end
          table_name
        end
      end

      def initialize_table(table, timestamp=Time.now.utc)
        table_name = table_name(table)
        with_context_logging("`initialize_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(initialize_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotInitialize.new(table) \
              if zero?(rows_affected)
          end
          table_name
        end
      end

      def lock_table(table, timestamp=Time.now.utc)
        table_name = table_name(table)
        with_context_logging("`lock_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(lock_table_sql(table, timestamp)).rows_affected
            raise Purview::Exceptions::CouldNotLock.new(table) \
              if zero?(rows_affected)
          end
          table_name
        end
      end

      def sync
        with_context_logging('`sync`') do
          timestamp = Time.now.utc
          with_next_table(timestamp) do |table|
            sync_table_with_lock(table, timestamp)
          end
        end
      end

      def sync_table(table)
        raise Purview::Exceptions::WrongDatabase.new(table) \
          unless tables.include?(table)
        table_name = table_name(table)
        with_context_logging("`sync_table` for: #{table_name}") do
          timestamp = Time.now.utc
          sync_table_with_lock(table, timestamp)
        end
      end

      def unlock_table(table)
        table_name = table_name(table)
        with_context_logging("`unlock_table` for: #{table_name}") do
          with_new_connection do |connection|
            rows_affected = \
              connection.execute(unlock_table_sql(table)).rows_affected
            raise Purview::Exceptions::CouldNotUnlock.new(table) \
              if zero?(rows_affected)
          end
          table_name
        end
      end

      private

      include Purview::Mixins::Connection
      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger

      attr_reader :opts

      public :connect

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

      def connection_type
        raise %{All "#{Base}(s)" must override the "connection_type" method}
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

      def create_index_sql(table_name, index_name, table, columns, index_opts={})
        raise %{All "#{Base}(s)" must override the "create_index_sql" method}
      end

      def create_table_sql(table_name, table, table_opts={})
        raise %{All "#{Base}(s)" must override the "create_table_sql" method}
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

      def dialect
        dialect_type.new
      end

      def dialect_type
        raise %{All "#{Base}(s)" must override the "dialect_type" method}
      end

      def disable_table_sql(table)
        raise %{All "#{Base}(s)" must override the "disable_table_sql" method}
      end

      def drop_index(table, columns, opts={})
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

      def drop_index_sql(table_name, index_name, table, columns, index_opts={})
        raise %{All "#{Base}(s)" must override the "drop_index_sql" method}
      end

      def drop_table_sql(table_name, table, table_opts={})
        raise %{All "#{Base}(s)" must override the "drop_table_sql" method}
      end

      def enable_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "enable_table_sql" method}
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

      def extract_index_options(opts)
        opts[:index] || {}
      end

      def extract_table_options(opts)
        opts[:table] || { :create_indices => true }
      end

      def false_value
        dialect.false_value
      end

      def get_enabled_at_for_table(connection, table)
        row = connection.execute(get_enabled_at_for_table_sql(table)).rows[0]
        timestamp = row[table_metadata_enabled_at_column_name]
        timestamp ? Time.parse(timestamp) : nil
      end

      def get_enabled_at_for_table_sql(table)
        raise %{All "#{Base}(s)" must override the "get_enabled_at_for_table_sql" method}
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
        timestamp ? Time.parse(timestamp) : nil
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
        table_name = row && row[table_metadata_table_name_column_name]
        table_name && tables_by_name[table_name]
      end

      def next_table_sql(timestamp)
        raise %{All "#{Base}(s)" must override the "next_table_sql" method}
      end

      def next_window(connection, table, timestamp)
        min = get_max_timestamp_pulled_for_table(connection, table)
        max = min + table.window_size
        now = timestamp
        min > now ? nil : Purview::Structs::Window.new(
          :min => min,
          :max => max > now ? now : max
        )
      end

      def null_value
        dialect.null_value
      end

      def nullable?(column)
        column.nullable?
      end

      def primary_key?(column)
        column.primary_key?
      end

      def quoted(value)
        dialect.quoted(value)
      end

      def sanitized(value)
        dialect.sanitized(value)
      end

      def set_enabled_at_for_table(connection, table, timestamp)
        connection.execute(set_enabled_at_for_table_sql(table, timestamp))
      end

      def set_enabled_at_for_table_sql(table, timestamp)
        raise %{All "#{Base}(s)" must override the "set_enabled_at_for_table_sql" method}
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
          with_new_transaction do |connection|
            table.sync(connection, window)
            set_last_pulled_at_for_table(
              connection,
              table,
              timestamp
            )
            set_max_timestamp_pulled_for_table(
              connection,
              table,
              window.max
            )
            last_window = window
          end
        end
        last_window
      end

      def table_metadata_enabled_at_column_definition
        column = Purview::Columns::Timestamp.new(table_metadata_enabled_at_column_name)
        column_definition(column)
      end

      def table_metadata_enabled_at_column_name
        'enabled_at'
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

      def table_name(table, table_opts={})
        table_opts[:name] || table.name
      end

      def tables
        @tables ||= Set.new.tap do |result|
          (opts[:tables] || []).each do |table|
            table.database = self if result.add?(table)
          end
        end
      end

      def tables_by_name
        @tables_by_name ||= {}.tap do |result|
          tables.each do |table|
            result[table.name] = table
          end
        end
      end

      def true_value
        dialect.true_value
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
          raise Purview::Exceptions::NoTable.new unless table
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
          raise Purview::Exceptions::NoWindow.new(table) unless window
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
