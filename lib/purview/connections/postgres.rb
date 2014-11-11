module Purview
  module Connections
    class Postgres < Base
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
