module Purview
  module Databases
    class SQLite < Base
      private

      def connection_type
        Purview::Connections::SQLite
      end

      def create_index_sql(table_name, index_name, index, index_opts={})
        'CREATE%sINDEX %s ON %s (%s)' % [
          index.unique? ? ' UNIQUE ' : ' ',
          index_name,
          table_name,
          column_names(index).join(', '),
        ]
      end

      def create_table_sql(table_name, table, table_opts={})
        'CREATE TABLE %s (%s)' % [
          table_name,
          column_definitions(table).join(', '),
        ]
      end

      def create_temporary_table_sql(table_name, table, table_opts={})
        'CREATE TEMPORARY TABLE %s (%s)' % [
          table_name,
          column_definitions(table).join(', '),
        ]
      end

      def dialect_type
        Purview::Dialects::SQLite
      end

      def disable_table_sql(table)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NOT NULL' % [
          table_metadata_table.name,
          table_metadata_table.enabled_at_column.name,
          null_value,
          table_metadata_table.table_name_column.name,
          quoted(table.name),
          table_metadata_table.enabled_at_column.name,
        ]
      end

      def drop_index_sql(table_name, index_name, index, index_opts={})
        'DROP INDEX %s' % [
          index_name,
        ]
      end

      def drop_table_sql(table_name, table, table_opts={})
        'DROP TABLE %s' % [
          table_name,
        ]
      end

      def enable_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NULL' % [
          table_metadata_table.name,
          table_metadata_table.enabled_at_column.name,
          quoted(timestamp),
          table_metadata_table.table_name_column.name,
          quoted(table.name),
          table_metadata_table.enabled_at_column.name,
        ]
      end

      def ensure_table_metadata_absent_for_table_sql(table)
        'DELETE FROM %s WHERE %s = %s' % [
          table_metadata_table.name,
          table_metadata_table.table_name_column.name,
          quoted(table.name),
        ]
      end

      def ensure_table_metadata_exists_for_table_sql(table)
        'INSERT INTO %s (%s) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s = %s)' % [
          table_metadata_table.name,
          table_metadata_table.table_name_column.name,
          quoted(table.name),
          table_metadata_table.name,
          table_metadata_table.table_name_column.name,
          quoted(table.name),
        ]
      end

      def ensure_table_metadata_table_exists_sql
        'CREATE TABLE IF NOT EXISTS %s (%s)' % [
          table_metadata_table.name,
          column_definitions(table_metadata_table).join(', '),
        ]
      end

      def initialize_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NULL' % [
          table_metadata_table.name,
          table_metadata_table.max_timestamp_pulled_column.name,
          quoted(timestamp),
          table_metadata_table.table_name_column.name,
          quoted(table.name),
          table_metadata_table.max_timestamp_pulled_column.name,
        ]
      end

      def get_table_metadata_value_sql(table, column)
        'SELECT %s FROM %s WHERE %s = %s' % [
          column.name,
          table_metadata_table.name,
          table_metadata_table.table_name_column.name,
          quoted(table.name),
        ]
      end

      def limit_map
        super.merge(Purview::Types::String => 255)
      end

      def lock_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NULL' % [
          table_metadata_table.name,
          table_metadata_table.locked_at_column.name,
          quoted(timestamp),
          table_metadata_table.table_name_column.name,
          quoted(table.name),
          table_metadata_table.locked_at_column.name,
        ]
      end

      def next_table_sql(timestamp)
        'SELECT %s FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL AND %s IS NULL ORDER BY %s IS NULL DESC, %s LIMIT 1' % [
          table_metadata_table.table_name_column.name,
          table_metadata_table.name,
          table_metadata_table.enabled_at_column.name,
          table_metadata_table.max_timestamp_pulled_column.name,
          table_metadata_table.locked_at_column.name,
          table_metadata_table.last_pulled_at_column.name,
          table_metadata_table.last_pulled_at_column.name,
        ]
      end

      def set_table_metadata_value_sql(table, column, value)
        'UPDATE %s SET %s = %s WHERE %s = %s' % [
          table_metadata_table.name,
          column.name,
          quoted(value),
          table_metadata_table.table_name_column.name,
          quoted(table.name),
        ]
      end

      def type_map
        super.merge(
          Purview::Types::Money => 'decimal',
          Purview::Types::UUID => 'varchar',
        )
      end

      def unlock_table_sql(table)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NOT NULL' % [
          table_metadata_table.name,
          table_metadata_table.locked_at_column.name,
          null_value,
          table_metadata_table.table_name_column.name,
          quoted(table.name),
          table_metadata_table.locked_at_column.name,
        ]
      end
    end
  end
end
