module Purview
  module Exceptions
    class CouldNotInitialize < BaseTable
      def message
        "Could not initialize table: #{table.name}"
      end
    end
  end
end
