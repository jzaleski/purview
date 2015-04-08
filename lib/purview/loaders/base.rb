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

      include Purview::Mixins::Logger

      attr_reader :opts

      def create_temporary_table(connection)
        database.create_table(
          connection,
          table,
          :table => {
            :name => table.temporary_name,
            :temporary => true,
          }
        )
      end

      def database
        table.database
      end

      def id_in_sql(temporary_table_name)
        'SELECT %s FROM %s' % [
          table.id_column.name,
          temporary_table_name,
        ]
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

      def quoted(value)
        database.quoted(value)
      end

      def row_values(row)
        table.column_names.map { |column_name| quoted(row[column_name]) }.join(', ')
      end

      def rows_per_slice
        opts[:rows_per_slice] || 1000
      end

      def table
        opts[:table]
      end

      def table_delete_sql(window, temporary_table_name)
        'DELETE FROM %s WHERE %s AND %s NOT IN (%s)' % [
          table.name,
          window_sql(window),
          table.id_column.name,
          id_in_sql(temporary_table_name),
        ]
      end
    end

    def table_insert_sql(window, temporary_table_name)
      'INSERT INTO %s (%s) SELECT %s FROM %s t1 WHERE NOT EXISTS (SELECT 1 FROM %s t2 WHERE t1.%s = t2.%s)' % [
        table.name,
        table.column_names.join(', '),
        table.column_names.join(', '),
        temporary_table_name,
        table.name,
        table.id_column.name,
        table.id_column.name,
      ]
    end

    def table_update_sql(window, temporary_table_name)
      'UPDATE %s t1 SET %s FROM %s t2 WHERE t1.%s = t2.%s' % [
        table.name,
        table.column_names.map { |column_name| "#{column_name} = t2.#{column_name}" }.join(', '),
        temporary_table_name,
          table.id_column.name,
          table.id_column.name,
      ]
    end

    def temporary_table_insert_sql(temporary_table_name, rows)
      'INSERT INTO %s (%s) VALUES %s' % [
        temporary_table_name,
        table.column_names.join(', '),
        rows.map { |row| "(#{row_values(row)})" }.join(', ')
      ]
    end

    def temporary_table_verify_sql(temporary_table_name, rows, window)
      'SELECT COUNT(1) FROM %s WHERE %s NOT BETWEEN %s AND %s' % [
        temporary_table_name,
        table.updated_timestamp_column.name,
        quoted(window.min),
        quoted(window.max),
      ]
    end

    def verify_temporary_table(connection, temporary_table_name, rows, window)
      with_context_logging("`verify_temporary_table` for: #{temporary_table_name}") do
        rows_outside_window = connection.execute(
          temporary_table_verify_sql(
            temporary_table_name,
            rows,
            window
          )
        ).rows[0][0]
        raise Purview::Exceptions::RowsOutsideWindow.new(temporary_table_name, rows_outside_window) \
          unless rows_outside_window.zero?
      end
    end

    def window_sql(window)
      '%s BETWEEN %s AND %s' % [
        table.updated_timestamp_column.name,
        quoted(window.min),
        quoted(window.max),
      ]
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
