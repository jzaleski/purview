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
        result = execute_sql(sql)
        Purview::Structs::Result.new(
          :rows => extract_rows(result),
          :rows_affected => extract_rows_affected(result)
        )
      end

      def with_transaction
        raise %{All "#{Base}(s)" must override the "with_transaction" method}
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts, :connection

      def execute_sql(sql)
        raise %{All "#{Base}(s)" must override the "execute_sql" method}
      end

      def extract_rows(result)
        raise %{All "#{Base}(s)" must override the "extract_rows" method}
      end

      def extract_rows_affected(result)
        raise %{All "#{Base}(s)" must override the "extract_rows_affected" method}
      end

      def new_connection
        raise %{All "#{Base}(s)" must override the "new_connection" method}
      end
    end
  end
end
