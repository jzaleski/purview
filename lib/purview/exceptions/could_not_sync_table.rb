module Purview
  module Exceptions
    class CouldNotSyncTable < BaseTable
      def message
        "Could not sync table: #{table.name}"
      end
    end
  end
end
