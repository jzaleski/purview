module Purview
  module Connections
    class MySQL < Base
      def with_transaction
        raw_connection.query(BEGIN_TRANSACTION)
        yield.tap { |result| raw_connection.query(COMMIT_TRANSACTION) }
      rescue Mysql2::Error
        raw_connection.query(ROLLBACK_TRANSACTION)
        raise
      end

      private

      BEGIN_TRANSACTION = 'BEGIN'
      COMMIT_TRANSACTION = 'COMMIT'
      ROLLBACK_TRANSACTION = 'ROLLBACK'

      def execute_sql(sql)
        raw_connection.query(sql, query_opts)
      end

      def extract_rows(result)
        result && result.to_a
      end

      def extract_rows_affected(result)
        raw_connection.affected_rows
      end

      def new_connection
        Mysql2::Client.new(opts)
      end

      def opts_map
        {
          :database => :database,
          :host => :host,
          :password => :password,
          :port => :port,
          :username => :username,
        }
      end

      def query_opts
        { :cast => false }
      end
    end
  end
end
