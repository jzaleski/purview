module Purview
  module RawConnections
    class Base
      def self.connect(opts)
        new(opts)
      end

      def self.with_new_connection(opts)
        yield connection = connect(opts)
      ensure
        connection.disconnect if connection
      end

      def initialize(opts)
        @opts = opts
        @raw_connection = new_connection
      end

      def disconnect
        raw_connection.close
        @raw_connection = nil
        self
      end

      def execute(sql, opts={})
        logger.debug("Executing: #{sql}")
        result = execute_sql(sql, opts)
        structify_result(result)
      end

      def with_transaction
        execute_sql(BEGIN_TRANSACTION)
        yield.tap { |result| execute_sql(COMMIT_TRANSACTION) }
      rescue
        execute_sql(ROLLBACK_TRANSACTION)
        raise
      end

      private

      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger

      BEGIN_TRANSACTION = 'BEGIN'
      COMMIT_TRANSACTION = 'COMMIT'
      ROLLBACK_TRANSACTION = 'ROLLBACK'

      attr_reader :opts, :raw_connection

      def database
        opts[:database]
      end

      def delete?(sql)
        !!(sql.to_s =~ /\ADELETE/i)
      end

      def execute_sql(sql, opts={})
        raise %{All "#{Base}(s)" must override the "execute_sql" method}
      end

      def extract_rows(result)
        raise %{All "#{Base}(s)" must override the "extract_rows" method}
      end

      def extract_rows_affected(result)
        raise %{All "#{Base}(s)" must override the "extract_rows_affected" method}
      end

      def host
        opts[:host]
      end

      def insert?(sql)
        !!(sql.to_s =~ /\AINSERT/i)
      end

      def new_connection
        raise %{All "#{Base}(s)" must override the "new_connection" method}
      end

      def password
        opts[:password]
      end

      def port
        opts[:port]
      end

      def select?(sql)
        !!(sql.to_s =~ /\ASELECT/i)
      end

      def structify_result(result)
        Purview::Structs::Result.new(
          :rows => structify_rows(extract_rows(result) || []),
          :rows_affected => extract_rows_affected(result)
        )
      end

      def structify_row(row)
        Purview::Structs::Row.new(row)
      end

      def structify_rows(rows)
        rows.map { |row| structify_row(row) }
      end

      def update?(sql)
        !!(sql.to_s =~ /\AUPDATE/i)
      end

      def username
        opts[:username]
      end
    end
  end
end
