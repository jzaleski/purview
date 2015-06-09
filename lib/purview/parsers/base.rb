module Purview
  module Parsers
    class Base
      def initialize(opts={})
        @opts = opts
        @table = table_opt
      end

      def parse(data)
        raise %{All "#{Base}(s)" must override the "parse" method}
      end

      def validate(data)
        raise %{All "#{Base}(s)" must override the "validate" method}
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts, :table

      def extract_headers(data)
        raise %{All "#{Base}(s)" must override the "extract_headers" method}
      end

      def extract_rows(data)
        raise %{All "#{Base}(s)" must override the "extract_rows" method}
      end

      def table_opt
        opts[:table]
      end
    end
  end
end
