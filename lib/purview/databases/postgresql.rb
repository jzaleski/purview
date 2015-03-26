module Purview
  module Databases
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
        database_type_map[column.class]
      end

      def database_type_map
        {
          Purview::Columns::Boolean => 'boolean',
          Purview::Columns::Date => 'date',
          Purview::Columns::Float => 'numeric',
          Purview::Columns::Integer => 'integer',
          Purview::Columns::Money => 'money',
          Purview::Columns::String => 'varchar',
          Purview::Columns::Text => 'text',
          Purview::Columns::Time => 'time',
          Purview::Columns::Timestamp => 'timestamp',
          Purview::Columns::UUID => 'uuid',
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
