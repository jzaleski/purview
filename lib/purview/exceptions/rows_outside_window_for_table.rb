module Purview
  module Exceptions
    class RowsOutsideWindowForTable < BaseTable
      def initialize(table, count)
        super(table)
        @count = count
      end

      def message
        "#{count} row(s) outside window for table: #{table.name}"
      end

      private

      attr_reader :count
    end
  end
end
