module Purview
  module Exceptions
    class CouldNotBaselineTable < BaseTable
      def message
        "Could not baseline table: #{table.name}"
      end
    end
  end
end
