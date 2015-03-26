module Purview
  module Connections
    class PostgreSQL < Base
      def connect
        @connection ||= PG.connect(opts)
        self
      end

      def disconnect
        connection.close
        @connection = nil
        self
      end

      def execute(sql)
        result = connection.exec(sql)
        rows = result && result.to_a
        rows_affected = result && result.cmd_tuples
        OpenStruct.new(:rows => rows, :rows_affected => rows_affected)
      end

      def with_transaction
        connection.transaction { yield }
      end

      private

      attr_reader :connection
    end
  end
end
