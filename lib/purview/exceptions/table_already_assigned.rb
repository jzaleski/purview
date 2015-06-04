module Purview
  module Exceptions
    class TableAlreadyAssigned < Base
      def initialize(index)
        @index = index
      end

      def message
        "Table already assigned for index on columns: #{index.table.column_names.join(', ')}"
      end

      private

      attr_reader :index
    end
  end
end
