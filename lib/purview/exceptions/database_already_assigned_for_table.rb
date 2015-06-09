module Purview
  module Exceptions
    class DatabaseAlreadyAssignedForTable < BaseTable
      def message
        "Database already assigned for table: #{table.name}"
      end
    end
  end
end
