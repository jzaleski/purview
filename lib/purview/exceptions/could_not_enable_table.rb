module Purview
  module Exceptions
    class CouldNotEnableTable < BaseTable
      def message
        "Could not enable table: #{table.name}"
      end
    end
  end
end
