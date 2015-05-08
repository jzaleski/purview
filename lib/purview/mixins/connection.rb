module Purview
  module Mixins
    module Connection
      def connect
        connection_type.new(connection_opts)
      end

      def with_new_connection
        connection_type.with_new_connection(connection_opts) { |connection| yield connection }
      end
    end
  end
end
