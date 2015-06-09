module Purview
  module Exceptions
    class CouldNotLockTable < BaseTable
      def message
        "Could not lock table: #{table.name}"
      end
    end
  end
end
