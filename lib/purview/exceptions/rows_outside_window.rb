module Purview
  module Exceptions
    class RowsOutsideWindow < Base
      def initialize(table_name, count)
        @table_name = table_name
        @count = count
      end

      def message
        "#{count} row(s) outside window for table: #{table_name}"
      end

      private

      attr_reader :count, :table_name
    end
  end
end
