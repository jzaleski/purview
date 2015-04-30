module Purview
  module Exceptions
    class LockAlreadyReleased < BaseTable
      def message
        "Lock already released for table: #{table.name}"
      end
    end
  end
end
