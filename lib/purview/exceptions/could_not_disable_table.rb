module Purview
  module Exceptions
    class CouldNotDisableTable < BaseTable
      def message
        "Could not disable table: #{table.name}"
      end
    end
  end
end
