module Purview
  module Pullers
    class BaseSQL < Base
      def pull(window)
        with_new_connection do |connection|
          connection.execute(pull_sql(window))
        end
      end

      private

      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger
      include Purview::Mixins::SQL

      def column_names
        table.column_names
      end

      def connect
        connection.connect
      end

      def connection
        connection_type.new(connection_opts)
      end

      def connection_opts
        {
          :database => database_name,
          :host => database_host,
          :password => database_password,
          :port => database_port,
          :username => database_username,
        }
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

      def null_value
        raise %{All "#{BaseSQL}(s)" must override the "null_value" method}
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
        opts[:table_name]
      end

      def with_new_connection
        yield connection = connect
      ensure
        connection.disconnect if connection
      end
    end
  end
end
