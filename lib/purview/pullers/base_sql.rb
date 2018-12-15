module Purview
  module Pullers
    class BaseSQL < Base
      def pull(window)
        with_new_connection do |connection|
          connection.execute(pull_sql(window) + additional_sql)
        end
      end

      private

      include Purview::Mixins::Connection
      include Purview::Mixins::Dialect
      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger

      def additional_sql
        " #{opts[:additional_sql]}".rstrip
      end

      def column_names
        table.columns.map do |column|
          name = column.name
          source_name = column.source_name
          source_name == name ? name : "#{source_name} AS #{name}"
        end
      end

      def connection_type
        raise %{All "#{BaseSQL}(s)" must override the "connection_type" method}
      end

      def database_host
        opts[:database_host]
      end

      def database_name
        opts[:database_name]
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

      def dialect_type
        raise %{All "#{BaseSQL}(s)" must override the "dialect_type" method}
      end

      def pull_sql(window)
        'SELECT %s FROM %s WHERE %s BETWEEN %s AND %s' % [
          column_names.join(', '),
          table_name,
          table.updated_timestamp_column.name,
          quoted(window.min),
          quoted(window.max),
        ]
      end

      def table_name
        opts[:table].name.gsub(/_raw$/, '')
      end
    end
  end
end
