module Purview
  module Exceptions
    class Table < Base
      def initialize(table)
        @table = table
      end

      private

      attr_reader :table
    end
  end
end
