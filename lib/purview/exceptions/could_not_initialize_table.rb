module Purview
  module Exceptions
    class CouldNotInitializeTable < BaseTable
      def message
        "Could not initialize table: #{table.name}"
      end
    end
  end
end
