module Purview
  module Exceptions
    class CouldNotUnlockTable < BaseTable
      def message
        "Could not unlock table: #{table.name}"
      end
    end
  end
end
