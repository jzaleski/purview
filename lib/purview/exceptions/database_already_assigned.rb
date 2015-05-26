module Purview
  module Exceptions
    class DatabaseAlreadyAssigned < BaseTable
      def message
        "Database already assigned for table: #{table.name}"
      end
    end
  end
end
