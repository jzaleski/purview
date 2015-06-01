module Purview
  module Exceptions
    class CouldNotEnable < BaseTable
      def message
        "Could not enable table: #{table.name}"
      end
    end
  end
end
