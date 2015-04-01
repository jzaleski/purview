module Purview
  module Connections
    class PostgreSQL < Base
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

      def new_connection
        PG.connect(opts)
      end
    end
  end
end
