module Purview
  module Mixins
    module Connection
      def connect
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

      def with_new_connection
        connection_type.with_new_connection(connection_opts) { |connection| yield connection }
      end

      def with_new_transaction
        with_new_connection do |connection|
          connection.with_transaction { yield connection }
        end
      end
    end
  end
end
