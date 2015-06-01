module Purview
  module Exceptions
    class WrongDatabase < BaseTable
      def message
        "Wrong database for table: #{table.name}"
      end
    end
  end
end
