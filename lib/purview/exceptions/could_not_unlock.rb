module Purview
  module Exceptions
    class CouldNotUnlock < BaseTable
      def message
        "Could not unlock table: #{table.name}"
      end
    end
  end
end
