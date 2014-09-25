module Purview
  module Exceptions
    class Base < StandardError; end

    class TableException < Base
      def initialize(table)
        @table = table
      end

      private

      attr_reader :table
    end

    class CouldNotAcquireLockException < TableException
      def message
        "Could not acquire the lock for table: #{table.name}"
      end
    end

    class LockAlreadyReleasedException < TableException
      def message
        "Lock already release for table: #{table.name}"
      end
    end

    class NoTableException < Base
      def message
        'Could not find a table'
      end
    end

    class NoWindowException < TableException
      def message
        "Could not find a window for table: #{table.name}"
      end
    end
  end
end
