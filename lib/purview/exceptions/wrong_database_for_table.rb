module Purview
  module Exceptions
    class WrongDatabaseForTable < BaseTable
      def message
        "Wrong database for table: #{table.name}"
      end
    end
  end
end
