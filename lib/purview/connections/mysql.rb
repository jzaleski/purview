module Purview
  module Connections
    class MySQL < Base
      def with_transaction
        connection.query(BEGIN_TRANSACTION)
        yield.tap { |result| connection.query(COMMIT_TRANSACTION) }
      rescue
        connection.query(ROLLBACK_TRANSACTION)
      end

      private

      BEGIN_TRANSACTION = 'BEGIN'
      COMMIT_TRANSACTION = 'COMMIT'
      ROLLBACK_TRANSACTION = 'ROLLBACK'

      def execute_sql(sql)
        connection.query(sql)
      end

      def extract_rows(result)
        result && result.to_a
      end

      def extract_rows_affected(result)
        result && result.affected_rows
      end

      def new_connection
        Mysql2::Client.new(opts)
      end
    end
  end
end
