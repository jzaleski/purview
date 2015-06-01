module Purview
  module Exceptions
    class CouldNotLock < BaseTable
      def message
        "Could not lock table: #{table.name}"
      end
    end
  end
end
