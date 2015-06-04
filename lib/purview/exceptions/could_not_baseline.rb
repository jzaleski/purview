module Purview
  module Exceptions
    class CouldNotBaseline < BaseTable
      def message
        "Could not baseline table: #{table.name}"
      end
    end
  end
end
