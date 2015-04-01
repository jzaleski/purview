module Purview
  module Connections
    class Base
      def initialize(opts={})
        @opts = opts
      end

      def connect
        @connection ||= new_connection
        self
      end

      def disconnect
        connection.close
        @connection = nil
        self
      end

      def execute(sql)
        raise %{All "#{Base}(s)" must override the "execute" method}
      end

      def with_transaction
        raise %{All "#{Base}(s)" must override the "with_transaction" method}
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts, :connection

      def new_connection
        raise %{All "#{Base}(s)" must override the "new_connection" method}
      end
    end
  end
end
