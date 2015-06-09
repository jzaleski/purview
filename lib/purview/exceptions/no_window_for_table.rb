module Purview
  module Exceptions
    class NoWindowForTable < BaseTable
      def message
        "Could not find a window for table: #{table.name}"
      end
    end
  end
end
