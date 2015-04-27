module Purview
  module Exceptions
    class LockAlreadyReleased < Table
      def message
        "Lock already released for table: #{table.name}"
      end
    end
  end
end
