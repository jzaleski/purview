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
  end
end
