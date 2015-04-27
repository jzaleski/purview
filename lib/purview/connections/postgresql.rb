module Purview
  module Connections
    class PostgreSQL < Base
      def with_transaction
        connection.transaction { yield }
      end

      private

      def execute_sql(sql)
        connection.exec(sql)
      end

      def extract_rows(result)
        result && result.to_a
      end

      def extract_rows_affected(result)
        result && result.cmd_tuples
      end

      def new_connection
        PG.connect(opts)
      end
    end
  end
end
