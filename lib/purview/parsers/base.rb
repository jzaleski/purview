module Purview
  module Parsers
    class Base
      def initialize(opts={})
        @opts = opts
      end

      def parse(data)
        raise %{All "#{Base}(s)" must override the "parse" method}
      end

      def validate(data)
        true
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts

      def build_result(row)
        {}.tap do |result|
          row.each do |key, value|
            if column = table.columns_by_name[key]
              result[key] = column.parse(value)
            else
              logger.debug(%{Unexpected column: "#{key}" in data-set})
            end
          end
        end
      end

      def table
        opts[:table]
      end
    end
  end
end
