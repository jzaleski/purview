module Purview
  module Connections
    class Base
      def initialize(opts={})
        @opts = opts
      end

      def connect
        raise %{All "#{Base}(s)" must override the "connect" method}
      end

      def disconnect
        raise %{All "#{Base}(s)" must override the "disconnect" method}
      end

      def execute(sql)
        raise %{All "#{Base}(s)" must override the "execute" method}
      end

      def with_transaction
        raise %{All "#{Base}(s)" must override the "with_transaction" method}
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts
    end

    class PostgreSQL < Base
      def connect
        @connection ||= PG.connect(opts)
        self
      end

      def disconnect
        @connection.close
        @connection = nil
      end

      def execute(sql)
        result = @connection.exec(sql)
        OpenStruct.new(:data => result ? result.to_a : nil, :rows_affected => result.cmd_tuples)
      end

      def with_transaction
        @connection.transaction { yield }
      end
    end
  end
end
