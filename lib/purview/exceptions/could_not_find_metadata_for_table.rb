module Purview
  module Exceptions
    class CouldNotFindMetadataForTable < BaseTable
      def message
        "Could not find metadata for table: #{table.name}"
      end
    end
  end
end
