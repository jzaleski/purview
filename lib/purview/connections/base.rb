module Purview
  module Connections
    class Base
      def initialize(opts={})
        @opts = map_opts(opts)
      end

      def connect
        @raw_connection ||= new_connection
        self
      end

      def disconnect
        raw_connection.close
        @raw_connection = nil
        self
      end

      def execute(sql)
        logger.debug("Executing: #{sql}")
        result = execute_sql(sql)
        Purview::Structs::Result.new(
          :rows => structify_rows(extract_rows(result) || []),
          :rows_affected => extract_rows_affected(result)
        )
      end

      def with_transaction
        raise %{All "#{Base}(s)" must override the "with_transaction" method}
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts, :raw_connection

      def execute_sql(sql)
        raise %{All "#{Base}(s)" must override the "execute_sql" method}
      end

      def extract_rows(result)
        raise %{All "#{Base}(s)" must override the "extract_rows" method}
      end

      def extract_rows_affected(result)
        raise %{All "#{Base}(s)" must override the "extract_rows_affected" method}
      end

      def map_opts(opts)
        opts_map.reduce({}) do |memo, (key1, key2)|
          value = opts[key1]
          memo[key2] = value if value
          memo
        end
      end

      def new_connection
        raise %{All "#{Base}(s)" must override the "new_connection" method}
      end

      def opts_map
        raise %{All "#{Base}(s)" must override the "opts_map" method}
      end

      def structify_row(row)
        Purview::Structs::Row.new(row)
      end

      def structify_rows(rows)
        rows.map { |row| structify_row(row) }
      end
    end
  end
end
