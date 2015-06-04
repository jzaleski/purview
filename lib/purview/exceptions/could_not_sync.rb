module Purview
  module Exceptions
    class CouldNotSync < BaseTable
      def message
        "Could not sync table: #{table.name}"
      end
    end
  end
end
