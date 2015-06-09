module Purview
  module Loaders
    class Base
      def initialize(opts={})
        @opts = opts
      end

      def load(connection, rows, window)
        with_context_logging("`load` for: #{table.name}") do
          with_temporary_table(connection, rows, window) do |temporary_table_name|
            update_result = \
              connection.execute(table_update_sql(window, temporary_table_name))
            delete_result = \
              connection.execute(table_delete_sql(window, temporary_table_name))
            insert_result = \
              connection.execute(table_insert_sql(window, temporary_table_name))
            logger.debug(
              '%d row(s) inserted, %d row(s) updated and %d row(s) deleted in: %s' % [
                insert_result.rows_affected,
                update_result.rows_affected,
                delete_result.rows_affected,
                table.name,
              ]
            )
          end
        end
      end

      private

      include Purview::Mixins::Dialect
      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger

      attr_reader :opts

      def count_column_name
        'count'
      end

      def create_temporary_table(connection)
        database.create_table(
          table,
          :connection => connection,
          :table => temporary_table_opts,
        )
      end

      def database
        table.database
      end

      def dialect_type
        raise %{All "#{Base}(s)" must override the "dialect_type" method}
      end

      def id_in_sql(temporary_table_name)
        raise %{All "#{Base}(s)" must override the "id_in_sql" method}
      end

      def in_window_sql(window)
        raise %{All "#{Base}(s)" must override the "in_window_sql" method}
      end

      def load_temporary_table(connection, temporary_table_name, rows, window)
        with_context_logging("`load_temporary_table` for: #{temporary_table_name}") do
          rows.each_slice(rows_per_slice) do |rows_slice|
            connection.execute(
              temporary_table_insert_sql(
                temporary_table_name,
                rows_slice
              )
            )
          end
        end
      end

      def not_in_window_sql(window)
        raise %{All "#{Base}(s)" must override the "not_in_window_sql" method}
      end

      def row_values(row)
        table.column_names.map { |column_name| quoted(sanitized(row[column_name])) }.join(', ')
      end

      def rows_per_slice
        opts[:rows_per_slice] || 1000
      end

      def table
        opts[:table]
      end

      def table_delete_sql(window, temporary_table_name)
        raise %{All "#{Base}(s)" must override the "table_delete_sql" method}
      end

      def table_insert_sql(window, temporary_table_name)
        raise %{All "#{Base}(s)" must override the "table_insert_sql" method}
      end

      def table_update_sql(window, temporary_table_name)
        raise %{All "#{Base}(s)" must override the "table_update_sql" method}
      end

      def temporary_table_insert_sql(temporary_table_name, rows)
        raise %{All "#{Base}(s)" must override the "temporary_table_insert_sql" method}
      end

      def temporary_table_opts
        {
          :create_indices => true,
          :name => table.temporary_name,
          :temporary => true,
        }
      end

      def temporary_table_verify_sql(temporary_table_name, rows, window)
        raise %{All "#{Base}(s)" must override the "temporary_table_verify_sql" method}
      end

      def verify_temporary_table(connection, temporary_table_name, rows, window)
        with_context_logging("`verify_temporary_table` for: #{temporary_table_name}") do
          rows_outside_window = connection.execute(
            temporary_table_verify_sql(
              temporary_table_name,
              rows,
              window
            )
          ).rows[0][count_column_name]
          raise Purview::Exceptions::RowsOutsideWindowForTable.new(table, rows_outside_window) \
            unless zero?(rows_outside_window)
        end
      end

      def with_temporary_table(connection, rows, window)
        yield(
          create_temporary_table(connection).tap do |temporary_table_name|
            load_temporary_table(
              connection,
              temporary_table_name,
              rows,
              window
            )
            verify_temporary_table(
              connection,
              temporary_table_name,
              rows,
              window
            )
          end
        )
      end
    end
  end
end
