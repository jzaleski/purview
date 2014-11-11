module Purview
  module Exceptions
    class NoWindow < Table
      def message
        "Could not find a window for table: #{table.name}"
      end
    end
  end
end
