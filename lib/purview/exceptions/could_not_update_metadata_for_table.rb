module Purview
  module Exceptions
    class CouldNotUpdateMetadataForTable < BaseTable
      def message
        "Could not update metadata for table: #{table.name}"
      end
    end
  end
end
