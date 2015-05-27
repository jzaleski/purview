module Purview
  module Databases
    class MySQL < Base
      private

      def connection_type
        Purview::Connections::MySQL
      end

      def create_index_sql(table_name, index_name, table, columns, index_opts={})
        'CREATE INDEX %s ON %s (%s)' % [
          index_name,
          table_name,
          column_names(columns).join(', '),
        ]
      end

      def create_table_sql(table_name, table, table_opts={})
        'CREATE TABLE %s (%s)' % [
          table_name,
          column_definitions(table).join(', '),
        ]
      end

      def create_temporary_table_sql(table_name, table, table_opts={})
        'CREATE TEMPORARY TABLE %s LIKE %s' % [
          table_name,
          table.name,
        ]
      end

      def default_map
        super.merge(Purview::Types::Timestamp => '0')
      end

      def dialect_type
        Purview::Dialects::MySQL
      end

      def disable_table_sql(table)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NOT NULL' % [
          table_metadata_table_name,
          table_metadata_enabled_at_column_name,
          null_value,
          table_metadata_table_name_column_name,
          quoted(table.name),
          table_metadata_enabled_at_column_name,
        ]
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

      def enable_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NULL' % [
          table_metadata_table_name,
          table_metadata_enabled_at_column_name,
          quoted(timestamp),
          table_metadata_table_name_column_name,
          quoted(table.name),
          table_metadata_enabled_at_column_name,
        ]
      end

      def ensure_table_metadata_absent_for_table_sql(table)
        'DELETE FROM %s WHERE %s = %s' % [
          table_metadata_table_name,
          table_metadata_table_name_column_name,
          quoted(table.name),
        ]
      end

      def ensure_table_metadata_exists_for_table_sql(table)
        'INSERT IGNORE INTO %s VALUES (%s, NULL, NULL, NULL, NULL)' % [
          table_metadata_table_name,
          quoted(table.name),
        ]
      end

      def ensure_table_metadata_table_exists_sql
        'CREATE TABLE IF NOT EXISTS %s (%s PRIMARY KEY, %s, %s, %s, %s)' % [
          table_metadata_table_name,
          table_metadata_table_name_column_definition,
          table_metadata_enabled_at_column_definition,
          table_metadata_last_pulled_at_column_definition,
          table_metadata_locked_at_column_definition,
          table_metadata_max_timestamp_pulled_column_definition,
        ]
      end

      def get_enabled_at_for_table_sql(table)
        'SELECT %s FROM %s WHERE %s = %s' % [
          table_metadata_enabled_at_column_name,
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

      def limit_map
        super.merge(Purview::Types::String => 255)
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
        'SELECT %s FROM %s WHERE %s IS NOT NULL AND %s IS NULL ORDER BY %s IS NULL DESC, %s LIMIT 1' % [
          table_metadata_table_name_column_name,
          table_metadata_table_name,
          table_metadata_enabled_at_column_name,
          table_metadata_locked_at_column_name,
          table_metadata_last_pulled_at_column_name,
          table_metadata_last_pulled_at_column_name,
        ]
      end

      def set_enabled_at_for_table_sql(table, timestamp)
        'UPDATE %s SET %s = %s WHERE %s = %s AND %s' % [
          table_metadata_table_name,
          table_metadata_enabled_at_column_name,
          quoted(timestamp),
          table_metadata_table_name_column_name,
          quoted(table.name),
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

      def type_map
        super.merge(
          Purview::Types::Money => 'decimal',
          Purview::Types::UUID => 'varchar',
        )
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
