module Purview
  module Exceptions
    class CouldNotAcquireLock < BaseTable
      def message
        "Could not acquire the lock for table: #{table.name}"
      end
    end
  end
end
