module Purview
  module Parsers
    class TSV < CSV
      private

      def column_separator
        "\t"
      end
    end
  end
end
