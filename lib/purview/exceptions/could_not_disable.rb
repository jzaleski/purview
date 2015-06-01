module Purview
  module Exceptions
    class CouldNotDisable < BaseTable
      def message
        "Could not disable table: #{table.name}"
      end
    end
  end
end
