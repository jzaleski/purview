module Purview
  module Exceptions
    class CouldNotFindTableMetadata < BaseTable
      def message
        "Could not find metadata for table: #{table.name}"
      end
    end
  end
end
