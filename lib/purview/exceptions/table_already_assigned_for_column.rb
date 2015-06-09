module Purview
  module Exceptions
    class TableAlreadyAssignedForColumn < Base
      def initialize(column)
        @column = column
      end

      def message
        "Table already assigned for column: #{column.name}"
      end

      private

      attr_reader :column
    end
  end
end
