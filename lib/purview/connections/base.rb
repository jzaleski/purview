module Purview
  module Connections
    class Base
      def self.connect(opts)
        new(opts)
      end

      def self.with_new_connection(opts)
        yield connection = connect(opts)
      ensure
        connection.disconnect if connection
      end

      def initialize(opts)
        @raw_connection = raw_connection_type.connect(opts)
      end

      def disconnect
        raw_connection.disconnect
        @raw_connection = nil
        self
      end

      def execute(sql)
        raw_connection.execute(sql)
      end

      def with_transaction
        raw_connection.with_transaction { yield }
      end

      private

      attr_reader :raw_connection

      def raw_connection_type
        raise %{All "#{Base}(s)" must override the "raw_connection_type" method}
      end
    end
  end
end
