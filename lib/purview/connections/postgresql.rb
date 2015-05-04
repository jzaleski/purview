module Purview
  module Connections
    class PostgreSQL < Base
      def with_transaction
        raw_connection.transaction { yield }
      end

      private

      def execute_sql(sql)
        raw_connection.exec(sql)
      end

      def extract_rows(result)
        result && result.to_a
      end

      def extract_rows_affected(result)
        result && result.cmd_tuples
      end

      def opts_map
        {
          :database => :dbname,
          :host => :host,
          :password => :password,
          :port => :port,
          :username => :user,
        }
      end

      def new_connection
        PG.connect(opts)
      end
    end
  end
end
