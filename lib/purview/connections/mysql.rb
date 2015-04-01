module Purview
  module Connections
    class MySQL < Base
      def execute(sql)
        result = connection.query(sql)
        rows = result && result.to_a
        rows_affected = connection.affected_rows
        OpenStruct.new(:rows => rows, :rows_affected => rows_affected)
      end

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

      def new_connection
        Mysql2::Client.new(opts)
      end
    end
  end
end
